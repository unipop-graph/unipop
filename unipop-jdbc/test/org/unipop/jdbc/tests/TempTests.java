package org.unipop.jdbc.tests;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import test.JdbcGraphProvider;
import test.JdbcOptimizedGraphProvider;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest.checkResults;
import static org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest.checkSideEffects;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.junit.Assert.*;

/**
 * @author Gur Ronen
 * @since 7/6/2016
 */
public class TempTests extends AbstractGremlinTest {
    public TempTests() throws SQLException, ClassNotFoundException {
        GraphManager.setGraphProvider(new JdbcGraphProvider());
    }

    @Test
    @LoadGraphWith
    public void shouldNeverPropagateANoBulkTraverser() {
        assertFalse(g.V().dedup().sideEffect(t -> t.asAdmin().setBulk(0)).hasNext());
        assertEquals(0, g.V().dedup().sideEffect(t -> t.asAdmin().setBulk(0)).toList().size());
        g.V().dedup().sideEffect(t -> t.asAdmin().setBulk(0)).sideEffect(t -> fail("this should not have happened")).iterate();
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_group_byXlabelX_selectXpersonX_unfold_outXcreatedX_name_limitX2X() {
        final Traversal<Vertex, String> traversal = g.V().out().<String, Vertex>group().by(T.label).select("person").unfold().out("created").<String>values("name").limit(2);
        printTraversalForm(traversal);
        checkResults(Arrays.asList("ripple", "lop"), traversal);
        checkSideEffects(traversal.asAdmin().getSideEffects());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void testA() {
        Traversal t = g.withPath().V().both().hasLabel("person").order().by("age", Order.decr).limit(5).values("name");
        check(t);
    }

    private void check(Traversal traversal) {
//        traversal.profile();
        System.out.println("pre-strategy:" + traversal);
        traversal.hasNext();
        System.out.println("post-strategy:" + traversal);

        int count = 0;
        while(traversal.hasNext()) {
            System.out.println(traversal.next());
            count ++;
        }
        System.out.println(count);
    }
}
