package com.tinkerpop.gremlin.elastic.elastic;


import com.tinkerpop.gremlin.elastic.ElasticService;
import com.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import com.tinkerpop.gremlin.elastic.tools.TimingAccessor;
import com.tinkerpop.gremlin.structure.*;
import org.apache.commons.configuration.BaseConfiguration;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

public class PerformanceTests {

    TimingAccessor sw = new TimingAccessor();

    @Test
    public void profile() throws IOException {
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty(Graph.GRAPH, ElasticGraph.class.getName());
        config.addProperty("elasticsearch.cluster.name", "test");
        config.addProperty("elasticsearch.index.name", "graph");
        config.addProperty("elasticsearch.refresh", true);
        config.addProperty("elasticsearch.client", ElasticService.ClientType.NODE.toString());

        startWatch("graph initalization");
        ElasticGraph graph = new ElasticGraph(config);
        stopWatch("graph initalization");

        startWatch("add vertices");
        int count = 10000;
        for(int i = 0; i < count; i++)
            graph.addVertex();
        stopWatch("add vertices");

        startWatch("vertex iterator");
        Iterator<Vertex> vertexIterator = graph.iterators().vertexIterator();
        stopWatch("vertex iterator");

        startWatch("add edges");
        vertexIterator.forEachRemaining(v -> v.addEdge("bla", v));
        stopWatch("add edges");

        startWatch("edge iterator");
        Iterator<Edge> edgeIterator = graph.iterators().edgeIterator();
        stopWatch("edge iterator");

        sw.print();
        System.out.println("-----");
        graph.elasticService.collectData();
        graph.close();
    }

    private void stopWatch(String s) {
        sw.timer(s).stop();
    }

    private void startWatch(String s) {
        sw.timer(s).start();
    }
}
