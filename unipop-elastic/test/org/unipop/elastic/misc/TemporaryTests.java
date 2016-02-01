package org.unipop.elastic.misc;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.unipop.elastic.ElasticGraphProvider;
import org.unipop.process.traversal.Text;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource.computer;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

public class TemporaryTests extends AbstractGremlinTest {

    public TemporaryTests() throws InterruptedException, ExecutionException, IOException {
        GraphManager.setGraphProvider(new ElasticGraphProvider());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_Drop() throws Exception {
        GraphTraversal traversal =  g.V().and(has("age", P.gt(27)), outE().count().is(P.gte(2l))).values("name");
        check(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX2X_inE() {
        GraphTraversal traversal = g.V().repeat(out()).times(2).valueMap();
        //GraphTraversal traversal = g.V().out().out().valueMap();

//        int counter = 0;
//
//        while(traversal.hasNext()) {
//            ++counter;
//            Vertex vertex = (Vertex)traversal.next();
//            Assert.assertTrue(vertex.value("name").equals("lop") || vertex.value("name").equals("ripple"));
//        }
//
//        Assert.assertEquals(2L, (long)counter);
//        Assert.assertFalse(traversal.hasNext());

        check(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeat() {
        GraphTraversal<Vertex, Vertex> traversal = g.V().repeat(out()).times(1);
        check(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void test() {
        GraphTraversal t = g.V().out();
        check(t);
    }

    private void check(GraphTraversal traversal) {
        System.out.println("pre-strategy:" + traversal);
        traversal.hasNext();
        System.out.println("post-strategy:" + traversal);

        //traversal.profile().cap(TraversalMetrics.METRICS_KEY);
        int count = 0;
        while(traversal.hasNext()) {
            System.out.println(traversal.next());
            count ++;
        }
        System.out.println(count);
    }
}
