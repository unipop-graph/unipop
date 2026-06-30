package org.unipop.elastic.tests;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;
import org.unipop.test.UnipopGraphProvider;
import test.ElasticGraphProvider;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

/**
 * Fast bounded smoke probe for the per-label write path: write two labelled vertices and a labelled
 * edge against the Testcontainers ES 8 instance, then read them back. Confirms per-label index
 * creation works for both vertex and edge labels (the "no such index [knows]" concern).
 */
public class ElasticSmokeTest {

    private Graph graph;

    @Before
    public void startUp() throws Exception {
        UnipopGraphProvider provider = new ElasticGraphProvider();
        Configuration configuration = provider.newGraphConfiguration(
                "smoke", this.getClass(), "smoke", new HashMap<>(), LoadGraphWith.GraphData.MODERN);
        this.graph = provider.openTestGraph(configuration);
    }

    @Test
    public void vertexAndEdgeRoundTrip() {
        Vertex marko = graph.addVertex(T.id, "1", T.label, "person", "name", "marko");
        Vertex lop = graph.addVertex(T.id, "2", T.label, "software", "name", "lop");
        marko.addEdge("created", lop, T.id, "10", "weight", 0.4);

        // Reads trigger DocumentController.search -> client.refresh() (flush + ES refresh).
        assertEquals(2L, (long) graph.traversal().V().count().next());
        assertEquals(1L, (long) graph.traversal().E().count().next());
        assertEquals("created", graph.traversal().E().next().label());
    }
}
