package org.unipop.process.strategyregistrar;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.process.coalesce.UniGraphCoalesceStepStrategy;
import org.unipop.process.edge.EdgeStepsStrategy;
import org.unipop.process.order.UniGraphOrderStrategy;
import org.unipop.process.properties.UniGraphPropertiesStrategy;
import org.unipop.process.range.UniGraphRangeStepStrategy;
import org.unipop.process.repeat.UniGraphRepeatStepStrategy;
import org.unipop.process.graph.UniGraphStepStrategy;
import org.unipop.process.union.UniGraphUnionStepStrategy;
import org.unipop.process.vertex.UniGraphVertexStepStrategy;
import org.unipop.process.vertex.UnboundedVertexAdjacencyStrategy;
import org.unipop.process.where.UniGraphWhereStepStrategy;

public class StandardStrategyProvider implements StrategyProvider {
    @Override
    public TraversalStrategies get() {
        DefaultTraversalStrategies traversalStrategies = new DefaultTraversalStrategies();
        traversalStrategies.addStrategies(
                new UnboundedVertexAdjacencyStrategy(),
                new UniGraphStepStrategy(),
                new UniGraphVertexStepStrategy(),
                new EdgeStepsStrategy(),
                new UniGraphPropertiesStrategy(),
                new UniGraphCoalesceStepStrategy(),
                new UniGraphWhereStepStrategy(),
                new UniGraphRepeatStepStrategy(),
                new UniGraphOrderStrategy(),
                new UniGraphRangeStepStrategy(),
                new UniGraphUnionStepStrategy());
        TraversalStrategies.GlobalCache.getStrategies(Graph.class).toList().forEach(traversalStrategies::addStrategies);
        return traversalStrategies;
    }
}

