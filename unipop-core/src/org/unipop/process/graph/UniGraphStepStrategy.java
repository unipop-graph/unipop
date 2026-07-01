package org.unipop.process.graph;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.process.predicate.PredicatesUtil;
import org.unipop.structure.UniGraph;

public class UniGraphStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy{

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if(TraversalHelper.onGraphComputer(traversal)) return;

        // Child/nested traversals have no graph bound; TinkerPop 3.8 applies provider strategies
        // recursively to them, so getGraph() may be empty — bail rather than throw NoSuchElement.
        Graph graph = traversal.getGraph().orElse(null);
        if(!(graph instanceof UniGraph)) {
            return;
        }

        UniGraph uniGraph = (UniGraph) graph;

        TraversalHelper.getStepsOfClass(GraphStep.class, traversal).forEach(graphStep -> {

            final UniGraphStep<?,?> uniGraphStep = new UniGraphStep<>(graphStep, uniGraph.getControllerManager());
            TraversalHelper.replaceStep(graphStep, uniGraphStep, traversal);
            PredicatesUtil.collectPredicates(uniGraphStep, traversal);
        });
    }
}

