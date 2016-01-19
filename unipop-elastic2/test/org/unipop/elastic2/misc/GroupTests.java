package org.unipop.elastic2.misc;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

/**
 * Created by Gilad on 05/11/2015.
 */
public class GroupTests extends AbstractGremlinTest {

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_group_byName_byAge_byMaxAge() throws Exception {
        GraphTraversal traversal =  g.V().has("age").group().by("name").by("age").by(count(Scope.local));

        printTraversalForm(traversal);

        //GraphTraversal traversal =  g.V().group().by("name").by("age").by(max(Scope.local));

        Object result = traversal.next();
        int x = 5;
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_E_group_byWeight_byWeight_byCount() throws Exception {
        GraphTraversal traversal =  g.E().group().by("weight").by("weight").by(count(Scope.local));

        printTraversalForm(traversal);

        Object result = traversal.next();
        int x = 5;
    }
}
