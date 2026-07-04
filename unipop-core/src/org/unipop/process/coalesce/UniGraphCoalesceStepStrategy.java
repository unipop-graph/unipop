package org.unipop.process.coalesce;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.AbstractLambdaTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CoalesceStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.process.coalesce.UniGraphCoalesceStep;
import org.unipop.structure.UniGraph;

/**
 * Created by sbarzilay on 3/15/16.
 */
public class UniGraphCoalesceStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        // getGraph() is empty for graph-less child traversals under TinkerPop 3.8 recursive strategy application.
        Graph graph = traversal.getGraph().orElse(null);
        if (!(graph instanceof UniGraph)) {
            return;
        }

        UniGraph uniGraph = (UniGraph) graph;

        TraversalHelper.getStepsOfClass(CoalesceStep.class, traversal).forEach(coalesceStep -> {
            // UniGraphCoalesceStep wraps each branch's output via UniGraphTraverserStep and tracks
            // the originating traverser through side-effects. Lambda branches — ValueTraversal
            // (values(k)/by(k)) and ConstantTraversal (constant(x)) — bypass the added step and
            // emit raw values, breaking that mechanism (ClassCastException value->Traverser / NPE).
            // These are common under ProductiveByStrategy, which rewrites by(x) as
            // coalesce(x, constant(..)). Leave such steps to TinkerPop's native CoalesceStep.
            boolean hasLambdaBranch = coalesceStep.getLocalChildren().stream()
                    .anyMatch(child -> child instanceof AbstractLambdaTraversal);
            if (hasLambdaBranch) return;
            // The side-effect-smuggling origin-removal in UniGraphCoalesceStep only works when the
            // step is a root step (its SIDE_EFFECTS requirement then forces a B_O_S_SE_SL_Traverser).
            // In a child/modulator traversal (project/order/local/union .by/child) the parent supplies
            // a bare B_O_Traverser whose getSideEffects() is the empty singleton, so the
            // (B_O_S_SE_SL_Traverser) cast throws (CCE) and an interface-based read would silently
            // break first-branch-wins. Native CoalesceStep is per-start and needs no side-effects, and
            // branch push-down is preserved (branches remain local children optimized by the other
            // provider strategies). Defer to it for non-root coalesce.
            if (!traversal.isRoot()) return;
            UniGraphCoalesceStep uniGraphCoalesceStep = new UniGraphCoalesceStep(coalesceStep.getTraversal(), uniGraph, coalesceStep.getLocalChildren());
            TraversalHelper.replaceStep(coalesceStep, uniGraphCoalesceStep, traversal);
        });
    }
}
