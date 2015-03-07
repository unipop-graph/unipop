package com.tinkerpop.gremlin.elastic.elastic;


import com.tinkerpop.gremlin.elastic.ElasticService;
import com.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

public class PerformanceTests {

    StopWatch sw = new StopWatch();

    @Test
    public void profile() throws IOException {
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty(Graph.GRAPH, ElasticGraph.class.getName());
        config.addProperty("elasticsearch.cluster.name", "test");
        String indexName = "graph";
        config.addProperty("elasticsearch.index.name", indexName.toLowerCase());
        config.addProperty("elasticsearch.refresh", true);
        config.addProperty("elasticsearch.client", ElasticService.ClientType.NODE.toString());

        startWatch();
        ElasticGraph graph = new ElasticGraph(config);
        stopWatch("graph initalization");

        startWatch();
        int count = 1000;
        for(int i = 0; i < count; i++)
            graph.addVertex();
        stopWatch("add vertices");

        startWatch();
        Iterator<Vertex> vertexIterator = graph.iterators().vertexIterator();
        stopWatch("vertex iterator");

        startWatch();
        vertexIterator.forEachRemaining(v -> v.addEdge("bla", v));
        stopWatch("add edges");

        startWatch();
        Iterator<Edge> edgeIterator = graph.iterators().edgeIterator();
        stopWatch("edge iterator");
    }

    private void stopWatch(String s) {
        sw.stop();
        System.out.println(s + ": " + sw.getTime()/1000f);
    }

    private void startWatch() {
        sw.reset();
        sw.start();
    }
}
