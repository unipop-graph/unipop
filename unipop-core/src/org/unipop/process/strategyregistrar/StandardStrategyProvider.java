package org.unipop.process.strategyregistrar;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.process.properties.UniGraphPropertiesStepStrategy;
import org.unipop.process.start.UniGraphStartStepStrategy;
import org.unipop.process.vertex.UniGraphVertexStepStrategy;

public class StandardStrategyProvider implements StrategyProvider {
    @Override
    public TraversalStrategies get() {
        DefaultTraversalStrategies traversalStrategies = new DefaultTraversalStrategies();
        traversalStrategies.addStrategies(
                new UniGraphStartStepStrategy(),
                new UniGraphVertexStepStrategy(),
                new UniGraphPropertiesStepStrategy());
        TraversalStrategies.GlobalCache.getStrategies(Graph.class).toList().forEach(traversalStrategies::addStrategies);
        return traversalStrategies;
    }
}

