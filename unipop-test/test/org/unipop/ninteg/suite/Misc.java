package org.unipop.ninteg.suite;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.elasticsearch.common.StopWatch;
import org.junit.Test;
import org.unipop.ninteg.IntegGraphProvider;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;

public class Misc extends AbstractGremlinTest {

    public Misc() throws Exception {
        GraphManager.setGraphProvider(new IntegGraphProvider());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_drop() throws Exception {
        final Traversal traversal = g.V().out().out().count();
        check(traversal);
    }

    private void check(Traversal traversal) {
        StopWatch sw = new StopWatch();
        sw.start();

        System.out.println("pre-strategy:" + traversal);
        traversal.hasNext();
        System.out.println("post-strategy:" + traversal);

        //traversal.profile().cap(TraversalMetrics.METRICS_KEY);

        while(traversal.hasNext())
            System.out.println(traversal.next());

        sw.stop();
        System.out.println(sw.toString());
    }

}
