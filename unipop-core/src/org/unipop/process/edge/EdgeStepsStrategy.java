package org.unipop.process.edge;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.unipop.structure.UniGraph;

/**
 * Created by sbarzilay on 6/8/16.
 */
public class EdgeStepsStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        UniGraph uniGraph = ((UniGraph) traversal.getGraph().get());

        TraversalHelper.getStepsOfClass(EdgeOtherVertexStep.class, traversal).forEach(edgeOtherVertexStep -> {
            UniGraphEdgeOtherVertexStep uniGraphEdgeOtherVertexStep = new UniGraphEdgeOtherVertexStep(traversal, uniGraph, uniGraph.getControllerManager());
            edgeOtherVertexStep.getLabels().forEach(uniGraphEdgeOtherVertexStep::addLabel);
            TraversalHelper.replaceStep(edgeOtherVertexStep, uniGraphEdgeOtherVertexStep, traversal);
        });

        TraversalHelper.getStepsOfClass(EdgeVertexStep.class, traversal).forEach(edgeVertexStep -> {
            UniGraphEdgeVertexStep uniGraphEdgeVertexStep = new UniGraphEdgeVertexStep(traversal, edgeVertexStep.getDirection(), uniGraph, uniGraph.getControllerManager());
            edgeVertexStep.getLabels().forEach(uniGraphEdgeVertexStep::addLabel);
            TraversalHelper.replaceStep(edgeVertexStep, uniGraphEdgeVertexStep, traversal);
        });
    }
}
