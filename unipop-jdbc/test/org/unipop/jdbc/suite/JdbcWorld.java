package org.unipop.jdbc.suite;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.features.World;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import test.JdbcGraphProvider;

import java.lang.annotation.Annotation;
import java.util.Map;

/**
 * TinkerPop 3.8 Gherkin/Cucumber {@link World} for the JDBC (H2-backed) Unipop provider.
 *
 * <p>The old JUnit {@code ProcessStandardSuite} process tests were removed in TinkerPop 3.8;
 * the process compliance suite is now driven by the shared {@code .feature} files in
 * {@code gremlin-test}. This World reuses Unipop's existing {@link JdbcGraphProvider}
 * machinery to build a {@code UniGraph} federated over H2 and to load the standard test
 * graph data (modern/grateful) for each scenario.</p>
 */
public class JdbcWorld implements World {

    private Graph currentGraph;

    @Override
    public GraphTraversalSource getGraphTraversalSource(final LoadGraphWith.GraphData graphData) {
        try {
            final JdbcGraphProvider provider = new JdbcGraphProvider();
            final Map<String, Object> baseConf =
                    provider.getBaseConfiguration("g", this.getClass(), "feature", graphData);
            final Configuration configuration = new MapConfiguration(baseConf);
            final Graph graph = provider.openTestGraph(configuration);

            // The H2 in-memory database is shared per-JVM; clear any state left by a prior
            // scenario before loading this scenario's data.
            provider.truncateTables();
            if (graphData != null) {
                provider.loadGraphData(graph, loadGraphWith(graphData), this.getClass(), "feature");
            }
            this.currentGraph = graph;
            return graph.traversal();
        } catch (final Exception ex) {
            throw new IllegalStateException("Could not build/load JDBC graph for " + graphData, ex);
        }
    }

    @Override
    public void afterEachScenario() {
        // Close the per-scenario graph so connection pools are not leaked across the suite.
        if (currentGraph != null) {
            try {
                currentGraph.close();
            } catch (final Exception ignored) {
            }
            currentGraph = null;
        }
    }

    /**
     * Unipop uses String element ids; the feature harness injects ids into Gremlin string queries,
     * so non-numeric ids must be quoted or the Gremlin grammar fails to parse them (e.g. UUIDs).
     */
    @Override
    public String convertIdToScript(final Object id, final Class<? extends Element> type) {
        return (id instanceof Number) ? id.toString() : "\"" + id + "\"";
    }

    /** Synthesize a {@link LoadGraphWith} annotation so we can reuse {@code loadGraphData}. */
    private static LoadGraphWith loadGraphWith(final LoadGraphWith.GraphData data) {
        return new LoadGraphWith() {
            @Override
            public Class<? extends Annotation> annotationType() {
                return LoadGraphWith.class;
            }

            @Override
            public GraphData value() {
                return data;
            }
        };
    }
}
