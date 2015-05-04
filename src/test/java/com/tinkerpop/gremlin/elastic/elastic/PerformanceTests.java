package com.tinkerpop.gremlin.elastic.elastic;


import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.elastic.ElasticGraphGraphProvider;
import com.tinkerpop.gremlin.elastic.elasticservice.*;
import com.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.*;
import org.apache.commons.configuration.BaseConfiguration;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__;

public class PerformanceTests {

    TimingAccessor sw = new TimingAccessor();


    @Test
    @LoadGraphWith(MODERN)
    public void testToPassTests() throws IOException, NoSuchMethodException {
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty("elasticsearch.cluster.name", "testgraph");
        String indexName = "graphtest10";
        config.addProperty("elasticsearch.index.name", indexName.toLowerCase());
        config.addProperty("elasticsearch.refresh", true);
        config.addProperty("elasticsearch.client", ElasticService.ClientType.NODE);
        ElasticGraph graph = new ElasticGraph(config);
        graph.elasticService.clearAllData();
        ElasticGraphGraphProvider elasticGraphProvider = new ElasticGraphGraphProvider();
        Method m = this.getClass().getMethod("testToPassTests");
        LoadGraphWith[] loadGraphWiths = m.getAnnotationsByType(LoadGraphWith.class);
        elasticGraphProvider.loadGraphData(graph, loadGraphWiths[0], this.getClass(), m.getName());
        //GraphTraversal<Vertex, Object> iter = graph.V().has("age").select("name");
        // GraphTraversal<Vertex, Element> iter = graph.V().has("age");
        // GraphTraversal<Vertex, Object> iter = graph.V().both().has(T.label, "software").values("name");
        //GraphTraversal<Vertex, Object> iter = graph.V().both().has(T.label, "software").dedup().by("lang").values("name");

        //GraphTraversal<Vertex, Element> iter = graph.V().has("name", (a, b) -> a.equals(b), "marko");
        startWatch("graph repeat");
//        GraphTraversal<Vertex, Map<String, Object>> iter = graph.V("1").as("a").out("knows").as("b").select();
        GraphTraversal<Vertex, Element> iter = graph.V("1", "2", "3").has(T.id, "2");
        System.out.println("iter = " + iter);
        while(iter.hasNext()){
            Object next = iter.next();
            String s = next.toString();
            System.out.println("s = " + s);
        }
        stopWatch("graph repeat");
        sw.print();


        int i =1 ;


        graph.elasticService.client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
        graph.close();
    }

    @Test
    public void profile() throws IOException {
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty("elasticsearch.cluster.name", "test");
        config.addProperty("elasticsearch.index.name", "graph");
        config.addProperty("elasticsearch.refresh", true);
        config.addProperty("elasticsearch.client", ElasticService.ClientType.NODE);

        startWatch("graph initalization");
        ElasticGraph graph = new ElasticGraph(config);
        stopWatch("graph initalization");
        graph.elasticService.clearAllData();

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
        graph.close();
    }

    @Test
    public void batchLoad() throws IOException {
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty("elasticsearch.cluster.name", "test");
        config.addProperty("elasticsearch.index.name", "graph");
        config.addProperty("elasticsearch.refresh", true);
        config.addProperty("elasticsearch.batch", true);
        config.addProperty("elasticsearch.client", ElasticService.ClientType.NODE);

        startWatch("graph initalization");
        ElasticGraph graph = new ElasticGraph(config);
        stopWatch("graph initalization");
        graph.elasticService.clearAllData();

        startWatch("add vertices");
        int count = 10000;
        for(int i = 0; i < count; i++)
            graph.addVertex();
        graph.commit();
        stopWatch("add vertices");


        startWatch("vertex iterator");
        Iterator<Vertex> vertexIterator = graph.iterators().vertexIterator();
        stopWatch("vertex iterator");

        startWatch("add edges");
        vertexIterator.forEachRemaining(v -> v.addEdge("bla", v));
        graph.commit();
        stopWatch("add edges");

        startWatch("edge iterator");
        Iterator<Edge> edgeIterator = graph.iterators().edgeIterator();
        stopWatch("edge iterator");

        sw.print();
        System.out.println("-----");
        graph.close();
    }

    private void stopWatch(String s) {
        sw.timer(s).stop();
    }

    private void startWatch(String s) {
        sw.timer(s).start();
    }
}
