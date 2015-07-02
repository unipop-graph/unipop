package org.elasticgremlin.testimpl;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.elasticgremlin.process.optimize.ElasticOptimizationStrategy;
import org.elasticgremlin.queryhandler.QueryHandler;
import org.elasticgremlin.structure.ElasticGraph;

import java.io.IOException;

public class ModernStarGraph extends ElasticGraph {
    static {
        TraversalStrategies.GlobalCache.registerStrategies(ModernStarGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(ElasticOptimizationStrategy.instance()));
    }

    public ModernStarGraph(Configuration configuration) throws IOException {
        super(configuration, ModernStarGraph::createModernQueryHandler);
    }

    public static QueryHandler createModernQueryHandler(ElasticGraph graph, Configuration configuration) {
        try {
            return new ModernStarGraphQueryHandler(graph, configuration);
        }
        catch (IOException e) {
            System.out.println("Failed to create ModernStarGraphQueryHandler");
            e.printStackTrace();
            return null;
        }
    }
}
