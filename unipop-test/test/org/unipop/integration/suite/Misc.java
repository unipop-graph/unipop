package org.unipop.integration.suite;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.junit.Test;
import org.unipop.integration.IntegrationGraphProvider;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;

public class Misc extends AbstractGremlinTest {

    public Misc() throws InterruptedException, ExecutionException, ClassNotFoundException, SQLException, IOException {
        GraphManager.setGraphProvider(new IntegrationGraphProvider());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_has_key() throws Exception {
        GraphTraversal traversal =  g.V(1);
        check(traversal);
    }

    private void check(GraphTraversal traversal) {
        System.out.println("pre-strategy:" + traversal);
        traversal.hasNext();
        System.out.println("post-strategy:" + traversal);

        //traversal.profile().cap(TraversalMetrics.METRICS_KEY);

        while(traversal.hasNext())
            System.out.println(traversal.next().getClass());
    }

}
