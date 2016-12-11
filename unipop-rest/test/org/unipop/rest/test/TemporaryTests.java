package org.unipop.rest.test;

import com.lordofthejars.nosqlunit.mongodb.ManagedMongoDb;
import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import test.RestGraphProvider;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static com.lordofthejars.nosqlunit.mongodb.ManagedMongoDb.MongoServerRuleBuilder.newManagedMongoDbRule;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Created by sbarzilay on 24/11/16.
 */
public class TemporaryTests extends AbstractGremlinTest {

    public TemporaryTests() throws Exception {
        GraphManager.setGraphProvider(new RestGraphProvider());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_hasXid_2_3X() {
        final Object id2 = convertToVertexId("vadas");
        final Object id3 = convertToVertexId("lop");
        final Traversal<Vertex, Vertex> traversal = g.V(convertToVertexId("marko")).out().hasId(id2, id3);
        assert_g_VX1X_out_hasXid_2_3X(id2, id3, traversal);
    }
    protected void assert_g_VX1X_out_hasXid_2_3X(Object id2, Object id3, Traversal<Vertex, Vertex> traversal) {
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertThat(traversal.next().id(), CoreMatchers.anyOf(is(id2), is(id3)));
        assertThat(traversal.next().id(), CoreMatchers.anyOf(is(id2), is(id3)));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void test() {
        Traversal t = g.V("1").out().hasId("2", "3");
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
