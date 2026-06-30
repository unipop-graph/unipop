package org.unipop.integration.suite;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.features.World;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.elastic.suite.EmbeddedElasticsearchServer;
import org.unipop.integration.IntegrationGraphProvider;

import java.lang.annotation.Annotation;
import java.util.Map;

public class IntegWorld implements World {

    private Graph currentGraph;

    @Override
    public GraphTraversalSource getGraphTraversalSource(final LoadGraphWith.GraphData graphData) {
        try {
            final IntegrationGraphProvider provider = new IntegrationGraphProvider();
            final Map<String, Object> baseConf =
                    provider.getBaseConfiguration("g", this.getClass(), "feature", graphData);
            final Configuration configuration = new MapConfiguration(baseConf);
            final Graph graph = provider.openTestGraph(configuration);
            // Assign BEFORE loading so afterEachScenario always closes this graph — even when a
            // scenario throws during loadGraphData (Phase-3 leak lesson).
            this.currentGraph = graph;

            // Federated reset: clear ES indices (elastic side) then rely on loadGraphData
            // to recreate tables/indices for both backends. The IntegrationGraphProvider.clear()
            // delegates to both elasticGraphProvider.clear() and jdbcGraphProvider.clear(), but
            // it requires a Configuration. We mirror RestWorld's approach: reset ES here and let
            // loadGraphData (which internally calls clear on both providers) recreate everything.
            EmbeddedElasticsearchServer.deleteAllIndices();

            if (graphData != null) {
                provider.loadGraphData(graph, loadGraphWith(graphData), this.getClass(), "feature");
            }
            return graph.traversal();
        } catch (final Exception ex) {
            throw new IllegalStateException("Could not build/load Integration graph for " + graphData, ex);
        }
    }

    @Override
    public void afterEachScenario() {
        if (currentGraph != null) {
            try { currentGraph.close(); } catch (final Exception ignored) {}
            currentGraph = null;
        }
    }

    @Override
    public String convertIdToScript(final Object id, final Class<? extends Element> type) {
        return (id instanceof Number) ? id.toString() : "\"" + id + "\"";
    }

    private static LoadGraphWith loadGraphWith(final LoadGraphWith.GraphData data) {
        return new LoadGraphWith() {
            @Override public Class<? extends Annotation> annotationType() { return LoadGraphWith.class; }
            @Override public GraphData value() { return data; }
        };
    }
}
