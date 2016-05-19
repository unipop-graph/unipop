package org.unipop.process.vertex;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.process.predicate.UniGraphPredicatesStrategy;
import org.unipop.process.properties.UniGraphPropertiesStepStrategy;
import org.unipop.structure.UniGraph;

import java.util.Set;

public class UniGraphVertexStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
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

        TraversalHelper.getStepsOfClass(VertexStep.class, traversal).forEach(vertexStep -> {
            UniGraphVertexStep uniGraphVertexStep = new UniGraphVertexStep(vertexStep, uniGraph.getControllerManager());

            TraversalHelper.replaceStep(vertexStep, uniGraphVertexStep, traversal);
        });
    }
}
