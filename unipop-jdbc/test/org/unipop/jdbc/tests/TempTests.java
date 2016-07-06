package org.unipop.jdbc.tests;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.junit.Test;
import org.unipop.jdbc.JdbcGraphProvider;

import java.sql.SQLException;

/**
 * @author Gur Ronen
 * @since 7/6/2016
 */
public class TempTests extends AbstractGremlinTest {
    public TempTests() throws SQLException, ClassNotFoundException {
        GraphManager.setGraphProvider(new JdbcGraphProvider());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void testA() {
        Traversal t = g.V("4").bothE().has("weight", P.lt(1d)).otherV();


        System.out.println(t.toList());
        int x = 6;
    }
}