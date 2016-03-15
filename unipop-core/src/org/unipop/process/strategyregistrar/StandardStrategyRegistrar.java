package org.unipop.process.strategyregistrar;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.process.predicate.UniGraphPredicatesStrategy;
import org.unipop.process.start.UniGraphStartStepStrategy;
import org.unipop.process.properties.UniGraphPropertiesStepStrategy;
import org.unipop.process.vertex.UniGraphVertexStepStrategy;
import org.unipop.structure.UniGraph;

public class StandardStrategyRegistrar implements StrategyRegistrar {
    @Override
    public void register() {
        try {
            DefaultTraversalStrategies strategies = new DefaultTraversalStrategies();
            strategies.addStrategies(
                    //add strategies here
                    new UniGraphStartStepStrategy(),
                    new UniGraphVertexStepStrategy(),
                    new UniGraphPredicatesStrategy(),
                    new UniGraphPropertiesStepStrategy()
            );

            TraversalStrategies.GlobalCache.getStrategies(Graph.class).toList().forEach(strategies::addStrategies);
            TraversalStrategies.GlobalCache.registerStrategies(UniGraph.class, strategies);
        } catch (Exception ex) {
            //TODO: something productive
        }
    }
}

