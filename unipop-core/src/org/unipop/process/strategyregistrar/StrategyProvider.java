package org.unipop.process.strategyregistrar;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;

public interface StrategyProvider {
    TraversalStrategies get();
}
