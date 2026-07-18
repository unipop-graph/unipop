package org.unipop.process.union;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TailGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.process.edge.EdgeStepsStrategy;
import org.unipop.process.properties.UniGraphPropertiesStrategy;
import org.unipop.process.repeat.UniGraphRepeatStepStrategy;
import org.unipop.process.graph.UniGraphStepStrategy;
import org.unipop.process.vertex.UniGraphVertexStepStrategy;
import org.unipop.structure.UniGraph;

import java.util.Set;

/**
 * Replaces a native {@link UnionStep} with the provider-level batched {@link UniGraphUnionStep} so a
 * branch's {@code UniGraphVertexStep} can batch the DB query for the whole input set in one round-trip.
 * Applies to BOTH a root {@code g.union(...)} and a child-option union (e.g. under {@code choose}/
 * {@code local}); the {@code isRoot()}-based {@code isStart} flag distinguishes the self-seeding root
 * case, and the else-branch below re-targets a union that lives inside a {@link TraversalParent}'s
 * children. Child unions are handled, not skipped.
 *
 * <p>The union is LEFT NATIVE when it can't be safely batched (see {@link #isUnbatchable} and the
 * {@code RepeatStep} guard).
 *
 * Created by sbarzilay on 3/23/16.
 */
public class UniGraphUnionStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    // A UniGraphUnionStep is a UniBulkStep: UniBulkStep.process() partitions the union input into
    // bulk.max-sized batches (default 100) and calls process(List) per batch, and
    // UniGraphUnionStep.process() calls branch.reset() at the TOP of each batch. So any branch step
    // whose correctness depends on seeing the WHOLE input stream (a global, cross-input-stateful step)
    // is reset between batches and only ever sees <=bulk.max inputs — diverging from native UnionStep,
    // which feeds ALL starts into one un-reset branch. For >bulk.max union inputs that silently returns
    // wrong results (e.g. union(out().limit(2)) yields up to 2 PER 100-batch, not 2 total). If ANY
    // branch contains such a step (anywhere, incl. nested), leave the union native.
    //
    // Barrier (TinkerPop 3.8.1) covers order/dedup/sample (CollectingBarrierStep->FilteringBarrier),
    // aggregate/group-side-effect (LocalBarrier), all reducing barriers count/sum/min/max/mean/fold/
    // group/groupCount (ReducingBarrierStep), and — via their *Contract -> FilteringBarrier — even
    // limit/range and tail. RangeGlobalStep/TailGlobalStep are checked explicitly too: they were plain
    // filters (NOT barriers) before 3.8, so the belt keeps the guard correct if that ever regresses.
    //
    // EXCEPT NoOpBarrierStep: TinkerPop's LazyBarrierStrategy inserts one after nearly every out()/in()
    // to enable bulking. It is a Barrier but RESULT-neutral — a per-batch reset only costs bulking
    // efficiency, never correctness — so excluding it keeps pure-adjacency unions on the batched path.
    private static boolean isUnbatchable(Step step) {
        return !(step instanceof NoOpBarrierStep)
                && (step instanceof Barrier || step instanceof RangeGlobalStep || step instanceof TailGlobalStep);
    }

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        return Sets.newHashSet(UniGraphStepStrategy.class, UniGraphVertexStepStrategy.class, UniGraphRepeatStepStrategy.class, EdgeStepsStrategy.class, UniGraphPropertiesStrategy.class);
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        // getGraph() is empty for graph-less child traversals under TinkerPop 3.8 recursive strategy application.
        Graph graph = traversal.getGraph().orElse(null);
        if (!(graph instanceof UniGraph)) {
            return;
        }

        UniGraph uniGraph = (UniGraph) graph;

        TraversalHelper.getStepsOfClass(UnionStep.class, traversal).forEach(unionStep -> {
            Traversal.Admin[] traversals = (Traversal.Admin[]) unionStep.getGlobalChildren().toArray(new Traversal.Admin[0]);
            // A union under RepeatStep is driven per-iteration and must stay native.
            if (traversal.getParent() instanceof RepeatStep)
                return;
            for (Traversal.Admin admin : traversals) {
                // Recursive: the global-stateful step can be nested (e.g. inside a nested union/map).
                if (TraversalHelper.anyStepRecursively(UniGraphUnionStepStrategy::isUnbatchable, admin))
                    return;
            }
            // Native g.union(...) builds UnionStep(isStart=true) — a root union self-seeds a starter
            // traverser. That is exactly a top-level (root) traversal whose start step IS the union;
            // a union that merely starts a child option traversal (e.g. under choose) still receives
            // real parent input and must NOT self-seed.
            boolean isStart = traversal.isRoot() && traversal.getStartStep() == unionStep;
            UniGraphUnionStep uniGraphUnionStep = new UniGraphUnionStep(traversal, uniGraph, isStart, traversals);
            if (TraversalHelper.stepIndex(unionStep, traversal) != -1) {
                TraversalHelper.replaceStep(unionStep, uniGraphUnionStep, traversal);
            } else {
                TraversalHelper.getStepsOfAssignableClass(TraversalParent.class, traversal).forEach(traversalParent -> {
                    traversalParent.getLocalChildren().forEach(child -> {
                        if(TraversalHelper.stepIndex(unionStep, child) != -1) {
                            TraversalHelper.replaceStep(unionStep, uniGraphUnionStep, child);
                        }
                    });
                    traversalParent.getGlobalChildren().forEach(child -> {
                        if(TraversalHelper.stepIndex(unionStep, child) != -1) {
                            TraversalHelper.replaceStep(unionStep, uniGraphUnionStep, child);
                        }
                    });
                });
            }
        });
    }
}
