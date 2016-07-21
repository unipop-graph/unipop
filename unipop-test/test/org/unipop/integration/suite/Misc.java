package org.unipop.integration.suite;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.common.StopWatch;
import org.junit.Test;
import org.unipop.integration.IntegrationGraphProvider;
import test.ElasticGraphProvider;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class Misc extends AbstractGremlinTest {
    public Misc() throws Exception {
        GraphManager.setGraphProvider(new IntegrationGraphProvider());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_EX11X() {
        GraphTraversal<Vertex, Vertex> t = graph.traversal().V().has("name", "josh").outE("created").as("e").inV().has("name", "lop").select("e");
        check(t);

        final Object edgeId = graph.traversal().V().has("name", "josh").outE("created").as("e").inV().has("name", "lop").<Edge>select("e").next().id();
        final Traversal<Edge, Edge> traversal = g.E(edgeId);
        assert_g_EX11X(edgeId, traversal);
    }


    private void assert_g_EX11X(final Object edgeId, final Traversal<Edge, Edge> traversal) {
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Edge e = traversal.next();
        assertEquals(edgeId, e.id());
        assertFalse(traversal.hasNext());
    }


    @LoadGraphWith(MODERN)
    @Test
    public void test1() throws Exception {
        Traversal t = g.V();
        check(t);

        Traversal t2 = g.V();
        check(t2);
    }

    @LoadGraphWith(MODERN)
    @Test
    public void test2() throws Exception {
        Traversal t = g.V();
        check(t);
    }

    private void check(Traversal traversal) {
        StopWatch sw = new StopWatch();
        int count = 0;
        sw.start();

        System.out.println("pre-strategy:" + traversal);
        traversal.hasNext();
        System.out.println("post-strategy:" + traversal);

        //traversal.profile().cap(TraversalMetrics.METRICS_KEY);

        while(traversal.hasNext()) {
            count ++;
            System.out.println(traversal.next());
        }

        sw.stop();
        System.out.println(sw.toString());
        System.out.println(count);
    }

}
