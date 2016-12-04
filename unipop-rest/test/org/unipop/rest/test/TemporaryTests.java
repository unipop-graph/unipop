package org.unipop.rest.test;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import test.ElasticGraphProvider;
import test.RestGraphProvider;

import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by sbarzilay on 24/11/16.
 */
public class TemporaryTests extends AbstractGremlinTest {

    public TemporaryTests() throws Exception {
        GraphManager.setGraphProvider(new RestGraphProvider());
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_V_hasLabelXsongX_order_byXperfomances_decrX_byXnameX_rangeX110_120X_name() {
        final Traversal<Vertex, String> traversal = g.V().hasLabel("song").order().by("performances", Order.decr).by("name").range(110, 120).values("name");
        printTraversalForm(traversal);
        final List<String> results = traversal.toList();
        assertEquals(10, results.size());
        assertEquals("WANG DANG DOODLE", results.get(0));
        assertEquals("THE ELEVEN", results.get(1));
        assertEquals("WAY TO GO HOME", results.get(2));
        assertEquals("FOOLISH HEART", results.get(3));
        assertEquals("GIMME SOME LOVING", results.get(4));
        assertEquals("DUPREES DIAMOND BLUES", results.get(5));
        assertEquals("CORRINA", results.get(6));
        assertEquals("PICASSO MOON", results.get(7));
        assertEquals("KNOCKING ON HEAVENS DOOR", results.get(8));
        assertEquals("MEMPHIS BLUES", results.get(9));
        assertFalse(traversal.hasNext());
    }

    private void assert_g_v2_in(final Traversal<Vertex, Vertex> traversal) {
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertEquals(traversal.next().value("name"), "marko");
        }
        assertEquals(1, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void test() {
        Traversal t = g.V().limit(10);
        check(t);
    }

    private void check(Traversal traversal) {
//        traversal.profile();
        System.out.println("pre-strategy:" + traversal);
        traversal.hasNext();
        System.out.println("post-strategy:" + traversal);

        int count = 0;
        while (traversal.hasNext()) {
            System.out.println(traversal.next());
            count++;
        }
        System.out.println(count);
    }
}
