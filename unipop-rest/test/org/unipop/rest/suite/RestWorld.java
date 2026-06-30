package org.unipop.rest.suite;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.features.World;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.elastic.suite.EmbeddedElasticsearchServer;
import test.RestGraphProvider;

import java.lang.annotation.Annotation;
import java.util.Map;

public class RestWorld implements World {

    private Graph currentGraph;

    @Override
    public GraphTraversalSource getGraphTraversalSource(final LoadGraphWith.GraphData graphData) {
        try {
            final RestGraphProvider provider = new RestGraphProvider();
            final Map<String, Object> baseConf =
                    provider.getBaseConfiguration("g", this.getClass(), "feature", graphData);
            final Configuration configuration = new MapConfiguration(baseConf);
            final Graph graph = provider.openTestGraph(configuration);
            // Assign BEFORE loading so afterEachScenario always closes this graph — even when a
            // scenario throws during loadGraphData (common for the partial REST provider). Otherwise
            // the graph's per-JVM ControllerManager DirectoryWatcher thread leaks on every failed
            // scenario, exhausting native threads ("unable to create native thread" OOM) across the run.
            this.currentGraph = graph;

            // ES is shared per-JVM; clear indices left by a prior scenario before loading.
            EmbeddedElasticsearchServer.deleteAllIndices();
            if (graphData != null) {
                provider.loadGraphData(graph, loadGraphWith(graphData), this.getClass(), "feature");
            }
            return graph.traversal();
        } catch (final Exception ex) {
            throw new IllegalStateException("Could not build/load Rest graph for " + graphData, ex);
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
