package org.unipop.process.edge;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.process.predicate.PredicatesUtil;
import org.unipop.structure.UniGraph;

/**
 * Created by sbarzilay on 6/8/16.
 */
public class EdgeStepsStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        // getGraph() is empty for graph-less child traversals under TinkerPop 3.8 recursive strategy application.
        Graph graph = traversal.getGraph().orElse(null);
        if (!(graph instanceof UniGraph)) {
            return;
        }
        UniGraph uniGraph = (UniGraph) graph;

        TraversalHelper.getStepsOfClass(EdgeOtherVertexStep.class, traversal).forEach(edgeOtherVertexStep -> {
            UniGraphEdgeOtherVertexStep uniGraphEdgeOtherVertexStep = new UniGraphEdgeOtherVertexStep(traversal, uniGraph, uniGraph.getControllerManager());
            edgeOtherVertexStep.getLabels().forEach(uniGraphEdgeOtherVertexStep::addLabel);
            TraversalHelper.replaceStep(edgeOtherVertexStep, uniGraphEdgeOtherVertexStep, traversal);
            uniGraphEdgeOtherVertexStep.setVertexPredicates(PredicatesUtil.collectVertexPredicates(uniGraphEdgeOtherVertexStep, traversal));
        });

        TraversalHelper.getStepsOfClass(EdgeVertexStep.class, traversal).forEach(edgeVertexStep -> {
            UniGraphEdgeVertexStep uniGraphEdgeVertexStep = new UniGraphEdgeVertexStep(traversal, edgeVertexStep.getDirection(), uniGraph, uniGraph.getControllerManager());
            edgeVertexStep.getLabels().forEach(uniGraphEdgeVertexStep::addLabel);
            TraversalHelper.replaceStep(edgeVertexStep, uniGraphEdgeVertexStep, traversal);
            uniGraphEdgeVertexStep.setVertexPredicates(PredicatesUtil.collectVertexPredicates(uniGraphEdgeVertexStep, traversal));
        });
    }
}
