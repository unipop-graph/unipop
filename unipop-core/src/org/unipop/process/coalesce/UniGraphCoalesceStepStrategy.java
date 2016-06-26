package org.unipop.process.coalesce;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
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
        Graph graph = traversal.getGraph().get();
        if (!(graph instanceof UniGraph)) {
            return;
        }

        UniGraph uniGraph = (UniGraph) graph;

        TraversalHelper.getStepsOfClass(CoalesceStep.class, traversal).forEach(coalesceStep -> {
            UniGraphCoalesceStep uniGraphCoalesceStep = new UniGraphCoalesceStep(coalesceStep.getTraversal(), uniGraph, coalesceStep.getLocalChildren());
            TraversalHelper.replaceStep(coalesceStep, uniGraphCoalesceStep, traversal);
        });
    }
}
