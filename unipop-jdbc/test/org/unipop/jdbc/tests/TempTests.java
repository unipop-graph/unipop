package org.unipop.jdbc.tests;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
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
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Gur Ronen
 * @since 7/6/2016
 */
public class TempTests extends AbstractGremlinTest {
    public TempTests() throws SQLException, ClassNotFoundException {
        GraphManager.setGraphProvider(new JdbcOptimizedGraphProvider());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.GRATEFUL)
    @Test
    public void testA() {
        Traversal t = g.V().repeat(both("followedBy")).times(2).group().by("songType").by(count());
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
