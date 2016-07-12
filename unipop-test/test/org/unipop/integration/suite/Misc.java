package org.unipop.integration.suite;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.elasticsearch.common.StopWatch;
import org.junit.Test;
import org.unipop.integration.IntegGraphProvider;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;

public class Misc extends AbstractGremlinTest {
    @LoadGraphWith(MODERN)
    @Test
    public void test1() throws Exception {
        Traversal t = g.V();
        check(t);

        Traversal t2 = g.V();
        check(t2);
    }

    @LoadGraphWith(MODERN)
    @Test
    public void test2() throws Exception {
        Traversal t = g.V();
        check(t);
    }

    private void check(Traversal traversal) {
        StopWatch sw = new StopWatch();
        int count = 0;
        sw.start();

        System.out.println("pre-strategy:" + traversal);
        traversal.hasNext();
        System.out.println("post-strategy:" + traversal);

        //traversal.profile().cap(TraversalMetrics.METRICS_KEY);

        while(traversal.hasNext()) {
            count ++;
            System.out.println(traversal.next());
        }

        sw.stop();
        System.out.println(sw.toString());
        System.out.println(count);
    }

}
