package org.unipop.elastic2.misc;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.unipop.controllerprovider.ControllerManagerFactory;
import org.unipop.elastic2.ElasticGraphProvider;
import org.unipop.elastic2.controllermanagers.BasicElasticControllerManager;
import org.unipop.elastic2.helpers.ElasticClientFactory;
import org.unipop.elastic2.helpers.ElasticHelper;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Created by andy.shiue on 15/01/2016.
 */
public class BasicTests {

    private Graph graph;
    private ElasticGraphProvider elasticGraphProvider;

    @Before
    public void startUp() throws InstantiationException, IOException, ExecutionException, InterruptedException {
        elasticGraphProvider = new ElasticGraphProvider();
        HashMap<String, Object> config = new HashMap<>();
        final Configuration configuration = elasticGraphProvider.newGraphConfiguration("testgraph", this.getClass(), "basicTests", config, LoadGraphWith.GraphData.MODERN);
        this.graph = elasticGraphProvider.openTestGraph(configuration);
    }

    @Test
    public void test() throws  InstantiationException {
        graph.addVertex(T.id, "v1", "field", "a", "field2", "c");
        graph.addVertex(T.id, "v2", "field", "b", "field2", "d");
        graph.addVertex(T.id, "v3", "field", "1", "field2", "d");
        Vertex v1 = graph.traversal().V("v1").next();
        Vertex v2 = graph.traversal().V("v2").next();
        Vertex v3 = graph.traversal().V("v3").next();
        v1.addEdge("1To2", v2);
        v1.addEdge("1To3", v3);
        GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V("v1").out("1To2").has("field", "b");
        Vertex vOutHas = traversal.next();
        assertEquals("a", v1.property("field").value());
        assertEquals("c", v1.property("field2").value());
        assertEquals("b", vOutHas.property("field").value());

        assertEquals(3, graph.traversal().V().toList().size());
        v2.remove();
        assertEquals(2, graph.traversal().V().toList().size());

    }

    @After
    public void shutDown() throws Exception {
        ElasticHelper.clearIndex(elasticGraphProvider.getClient(), "testgraph");
        graph.close();
    }
}
