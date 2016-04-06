package org.unipop.elastic.tests;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;
import org.unipop.common.test.UnipopGraphProvider;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ConfigurationTests {

    private Graph graph;

    @Before
    public void startUp() throws InstantiationException, IOException, ExecutionException, InterruptedException {
        UnipopGraphProvider unipopGraphProvider = new UnipopGraphProvider();
        HashMap<String, Object> config = new HashMap<>();
        config.put("elasticsearch.upsert", true);
        final Configuration configuration = unipopGraphProvider.newGraphConfiguration("testGraph", this.getClass(), "spatialTests",
                config, LoadGraphWith.GraphData.MODERN);
        this.graph = unipopGraphProvider.openTestGraph(configuration);
    }

    @Test
    public void upsertConfiguration() throws  InstantiationException {
        graph.addVertex(T.id, "1", "field", "a", "field2", "c");
        graph.addVertex(T.id, "1", "field", "b");

        GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V("1");
        Vertex vertex = traversal.next();
        assertEquals("b", vertex.property("field").value());
        assertEquals("c", vertex.property("field2").value());
        assertFalse(traversal.hasNext());
    }
}
