package org.elasticgremlin.elastic;

import org.apache.tinkerpop.gremlin.*;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.*;
import org.elasticgremlin.ElasticGraphGraphProvider;
import org.elasticgremlin.process.optimize.ForceFeedStep;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;

public class TemporaryTests extends AbstractGremlinTest {

    public TemporaryTests() throws InterruptedException, ExecutionException, IOException {
        GraphManager.setGraphProvider(new ElasticGraphGraphProvider());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_Drop() throws Exception {
        GraphTraversal traversal =  g.V().and(has("age", P.gt(27)), outE().count().is(P.gte(2l))).values("name");
        check(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void get_g_VX1X_out_hasIdX2X() {
        GraphTraversal traversal =   g.V("1").out().hasId("2");
        check(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeat() {
        GraphTraversal traversal = g.V();
        traversal.asAdmin().addStep(new ForceFeedStep(traversal.asAdmin()));
        traversal.repeat(__.out()).times(1);
        //GraphTraversal traversal = g.V().out();
        check(traversal);
    }

    private void check(GraphTraversal traversal) {
        System.out.println("pre-strategy:" + traversal);
        traversal.hasNext();
        System.out.println("post-strategy:" + traversal);

        //traversal.profile().cap(TraversalMetrics.METRICS_KEY);

        while(traversal.hasNext())
            System.out.println(traversal.next());
    }
}
