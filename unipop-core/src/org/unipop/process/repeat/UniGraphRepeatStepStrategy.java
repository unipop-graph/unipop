package org.unipop.process.repeat;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.process.graph.UniGraphStepStrategy;
import org.unipop.process.vertex.UniGraphVertexStepStrategy;
import org.unipop.structure.UniGraph;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by sbarzilay on 3/30/16.
 */
public class UniGraphRepeatStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        Set<Class<? extends TraversalStrategy.ProviderOptimizationStrategy>> priorStrategies = new HashSet<>();
        priorStrategies.add(UniGraphStepStrategy.class);
        priorStrategies.add(UniGraphVertexStepStrategy.class);
        return priorStrategies;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal)) return;

        // getGraph() is empty for graph-less child traversals under TinkerPop 3.8 recursive strategy application.
        Graph graph = traversal.getGraph().orElse(null);
        if (!(graph instanceof UniGraph)) {
            return;
        }

        UniGraph uniGraph = (UniGraph) graph;

        TraversalHelper.getStepsOfClass(RepeatStep.class, traversal).forEach(repeatStep -> {
            // A bare emit()/until()/times() with no repeat(..) body has an empty global-children
            // list. Leave it to the native RepeatStep, which validates and throws the expected
            // "The repeat()-traversal was not defined" error (indexing get(0) here would instead
            // throw an opaque IndexOutOfBoundsException).
            if (repeatStep.getGlobalChildren().isEmpty()) {
                return;
            }
            Traversal.Admin<?, ?> repeatBody = (Traversal.Admin) repeatStep.getGlobalChildren().get(0);
            if (TraversalHelper.hasStepOfClass(UnionStep.class, repeatBody)) {
                return;
            }
            // UniGraphRepeatStep pull-feeds traversers into the body one loop at a time, which breaks
            // any global Barrier step in the body (order/limit/range/tail/aggregate/barrier): those
            // need the whole per-iteration input at once, so their state leaks across loops and the
            // step silently drops results. Leave repeats with a barrier body to the native
            // RepeatStep, which re-evaluates the body cleanly each iteration.
            if (TraversalHelper.hasStepOfAssignableClass(Barrier.class, repeatBody)) {
                return;
            }
            // UniGraphRepeatStep does not manage the nested-loop stack, so nested repeats
            // (repeat(repeat(..)), repeat().until(repeat(..)), emit(repeat(..)), or a repeat inside
            // another repeat) hit EmptyStackException in NL_SL_Traverser.incrLoops(). It also does
            // not register named loops, so repeat("a", ..) with loops("a") throws
            // "loop name not defined". Leave both to TinkerPop's native RepeatStep, which handles them.
            if (repeatStep.getLoopName() != null || involvesNestedRepeat(repeatStep)) {
                return;
            }
            UniGraphRepeatStep uniGraphRepeatStep = new UniGraphRepeatStep(repeatStep, traversal.asAdmin(), uniGraph);
            if (repeatStep.getUntilTraversal() != null && TraversalHelper.getFirstStepOfAssignableClass(ReducingBarrierStep.class, repeatStep.getUntilTraversal()).isPresent())
                return;
            Traversal.Admin<?, ?> repeatTraversal = uniGraphRepeatStep.getRepeatTraversal();
            TraversalHelper.replaceStep(repeatStep, uniGraphRepeatStep, traversal);
            TraversalHelper.getStepsOfClass(RepeatStep.RepeatEndStep.class, repeatTraversal).forEach(repeatEndStep -> {
                UniGraphRepeatStep.RepeatEndStep uniGraphRepeatEndStep = new UniGraphRepeatStep.RepeatEndStep(repeatTraversal, uniGraphRepeatStep);
                TraversalHelper.replaceStep(repeatEndStep, uniGraphRepeatEndStep, repeatTraversal);
            });
        });

    }

    /**
     * True when this repeat step participates in nesting: its repeat/until/emit children contain
     * another {@link RepeatStep} (recursively), or the step itself sits inside another
     * {@link RepeatStep}. UniGraphRepeatStep cannot manage the nested loop stack, so these are left
     * to the native RepeatStep.
     */
    private static boolean involvesNestedRepeat(RepeatStep<?> repeatStep) {
        if (!repeatStep.getGlobalChildren().isEmpty()
                && TraversalHelper.hasStepOfAssignableClassRecursively(
                        RepeatStep.class, (Traversal.Admin) repeatStep.getGlobalChildren().get(0)))
            return true;
        if (repeatStep.getUntilTraversal() != null
                && TraversalHelper.hasStepOfAssignableClassRecursively(RepeatStep.class, repeatStep.getUntilTraversal()))
            return true;
        if (repeatStep.getEmitTraversal() != null
                && TraversalHelper.hasStepOfAssignableClassRecursively(RepeatStep.class, repeatStep.getEmitTraversal()))
            return true;
        // Walk up the parent chain: is this repeat contained within another repeat?
        Step<?, ?> current = repeatStep;
        while (true) {
            TraversalParent parent = current.getTraversal().getParent();
            if (parent == null || parent instanceof EmptyStep) return false;
            if (parent instanceof RepeatStep) return true;
            current = parent.asStep();
            if (current == null || current instanceof EmptyStep) return false;
        }
    }
}
