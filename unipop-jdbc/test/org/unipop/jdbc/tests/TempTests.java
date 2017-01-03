package org.unipop.jdbc.tests;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.IgnoreEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
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
        GraphManager.setGraphProvider(new JdbcOptimizedGraphProvider());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_properties_hasXid_nameIdX_value() {
        final Traversal<Vertex, Object> traversal = get_g_V_hasXageX_properties_hasXid_nameIdX_value(convertToVertexPropertyId("marko", "name").next());
        printTraversalForm(traversal);
        checkResults(Collections.singletonList("marko"), traversal);
    }

    public Traversal<Vertex, Object> get_g_V_hasXageX_properties_hasXid_nameIdX_value(final Object nameId) {
        return g.V().has("age").properties().has(T.id, nameId).value();
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void testA() {
        Traversal t = g.V("1").out().path().by("age").by("name");
        check(t);
    }

    private void check(Traversal traversal) {
//        traversal.profile();
        System.out.println("pre-strategy:" + traversal);
        traversal.hasNext();
        System.out.println("post-strategy:" + traversal);

        int count = 0;
        while(traversal.hasNext()) {
            Object next = traversal.next();
            System.out.println(next);
            count ++;
        }
        System.out.println(count);
    }
}
