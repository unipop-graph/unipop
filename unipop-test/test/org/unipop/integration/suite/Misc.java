package org.unipop.integration.suite;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.elasticsearch.common.StopWatch;
import org.junit.Assert;
import org.junit.Test;
import org.unipop.integration.IntegrationGraphProvider;
import org.unipop.integration.JsonGraphProvider;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class Misc extends AbstractGremlinTest {

    public Misc() throws InterruptedException, ExecutionException, ClassNotFoundException, SQLException, IOException {
        GraphManager.setGraphProvider(new JsonGraphProvider());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_drop() throws Exception {
        final Traversal traversal = g.V().out("knows", "created");
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
