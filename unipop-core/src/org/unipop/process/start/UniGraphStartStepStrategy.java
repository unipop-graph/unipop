package org.unipop.process.start;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.process.predicate.UniGraphPredicatesStrategy;
import org.unipop.process.properties.UniGraphPropertiesStepStrategy;
import org.unipop.structure.UniGraph;

import java.util.Collections;
import java.util.Set;

public class UniGraphStartStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy{
    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPost() {
        return Sets.newHashSet(UniGraphPredicatesStrategy.class, UniGraphPropertiesStepStrategy.class);
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if(traversal.getEngine().isComputer()) {
            return;
        }

        Graph graph = traversal.getGraph().get();
        if(!(graph instanceof UniGraph)) {
            return;
        }

        UniGraph uniGraph = (UniGraph) graph;

        TraversalHelper.getStepsOfClass(GraphStep.class, traversal).forEach(graphStep -> {
            if(graphStep.getIds().length > 0) return; //let Graph.vertices(ids) handle it.
            final UniGraphStartStep<?,?> uniGraphStartStep = new UniGraphStartStep<>(graphStep, uniGraph.getControllerManager());
            TraversalHelper.replaceStep(graphStep, (Step) uniGraphStartStep, traversal);
        });
    }
}

