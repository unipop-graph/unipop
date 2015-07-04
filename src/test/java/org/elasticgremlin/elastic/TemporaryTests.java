package org.elasticgremlin.elastic;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.*;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.elasticgremlin.ElasticGraphGraphProvider;
import org.junit.*;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;

public class TemporaryTests {

    private Graph graph;
    private GraphTraversalSource g;

    @Before
    public void startUp() throws InstantiationException, IOException, ExecutionException, InterruptedException {
        ElasticGraphGraphProvider elasticGraphProvider = new ElasticGraphGraphProvider();
        final Configuration configuration = elasticGraphProvider.newGraphConfiguration("testGraph", this.getClass(), "testToPassTests", LoadGraphWith.GraphData.MODERN);
        this.graph = elasticGraphProvider.openTestGraph(configuration);
        this.g = graph.traversal();
    }

    @Test
    public void g_V_Drop() throws Exception {
        GraphTraversal iter =  g.V().and(has("age", P.gt(27)), outE().count().is(P.gte(2l))).values("name");
        check(iter);
    }

    private void check(GraphTraversal traversal) {
        System.out.println("pre-strategy:" + traversal);
        traversal.hasNext();
        System.out.println("post-strategy:" + traversal);

        //traversal.profile().cap(TraversalMetrics.METRICS_KEY);

        while(traversal.hasNext()){
            Object next = traversal.next();
            String s = next.toString();
            System.out.println("s = " + s);
        }
    }
}
