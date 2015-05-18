package org.apache.tinkerpop.gremlin.elastic.elastic;


import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.elastic.ElasticGraphGraphProvider;
import org.apache.tinkerpop.gremlin.elastic.elasticservice.*;
import org.apache.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.*;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Iterator;

public class PerformanceTests {

    TimingAccessor sw = new TimingAccessor();

    /*@Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldPersistDataOnClose() throws Exception {
        final GraphProvider graphProvider = new ElasticGraphGraphProvider();
        Graph graph = graphProvider.standardTestGraph(this.getClass(), "shouldPersistDataOnClose");;

        final Vertex v = graph.addVertex();
        final Vertex u = graph.addVertex();
        if (graph.features().edge().properties().supportsStringValues()) {
            v.property("name", "marko");
            u.property("name", "pavel");
        }

        final Edge e = v.addEdge(graphProvider.convertLabel("collaborator"), u);
        if (graph.features().edge().properties().supportsStringValues())
            e.property("location", "internet");

        graph.close();

        final Graph reopenedGraph = graphProvider.standardTestGraph(this.getClass(), "shouldPersistDataOnClose");

        if (graph.features().vertex().properties().supportsStringValues()) {
            reopenedGraph.vertices().forEachRemaining(vertex -> {
                assertTrue(vertex.property("name").value().equals("marko") || vertex.property("name").value().equals("pavel"));
            });
        }

        reopenedGraph.edges().forEachRemaining(edge -> {
            assertEquals(graphProvider.convertId("collaborator"), edge.label());
            if (graph.features().edge().properties().supportsStringValues())
                assertEquals("internet", edge.property("location").value());
        });

        graphProvider.clear(reopenedGraph, graphProvider.standardGraphConfiguration(this.getClass(), "shouldPersistDataOnClose"));
    }*/

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void testToPassTests() throws IOException, NoSuchMethodException {
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty("elasticsearch.cluster.name", "testgraph");
        String indexName = "graphtest14";
        config.addProperty("elasticsearch.index.name", indexName.toLowerCase());
        config.addProperty("elasticsearch.refresh", true);
        config.addProperty("elasticsearch.client", ElasticService.ClientType.NODE);
        ElasticGraph graph = new ElasticGraph(config);
        graph.elasticService.clearAllData();
        ElasticGraphGraphProvider elasticGraphProvider = new ElasticGraphGraphProvider();
        Method m = this.getClass().getMethod("testToPassTests");
        LoadGraphWith[] loadGraphWiths = m.getAnnotationsByType(LoadGraphWith.class);
        elasticGraphProvider.loadGraphData(graph, loadGraphWiths[0], this.getClass(), m.getName());
        GraphTraversalSource g = graph.traversal();


        /*GraphTraversal<Vertex, Path> iter = g.V().out().outE().inV().inE().inV().both().values("name").path();
        printTraversalForm(iter);
        //iter.profile().cap(TraversalMetrics.METRICS_KEY);

        System.out.println("iter = " + iter);
        while(iter.hasNext()){
            Object next = iter.next();
            String s = next.toString();
            System.out.println("s = " + s);
        }*/

        GraphTraversal<Vertex, String> iter2 = g.V().out().outE().inV().inE().inV().both().values("name");
        printTraversalForm(iter2);
        //iter.profile().cap(TraversalMetrics.METRICS_KEY);

        System.out.println("iter2 = " + iter2);
        while(iter2.hasNext()){
            Object next = iter2.next();
            String s = next.toString();
            System.out.println("s = " + s);
        }


        graph.elasticService.client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
        graph.close();
    }

    public void printTraversalForm(final Traversal traversal) {
        System.out.println("   pre-strategy:" + traversal);
        traversal.hasNext();
        System.out.println("  post-strategy:" + traversal);
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
        Iterator<Vertex> vertexIterator = graph.vertices();
        stopWatch("vertex iterator");

        startWatch("add edges");
        vertexIterator.forEachRemaining(v -> v.addEdge("bla", v));
        stopWatch("add edges");

        startWatch("edge iterator");
        Iterator<Edge> edgeIterator = graph.edges();
        stopWatch("edge iterator");

        sw.print();
        System.out.println("-----");
        graph.close();
    }

    /*@Test
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
        Iterator<Vertex> vertexIterator = graph.vertices();
        stopWatch("vertex iterator");

        startWatch("add edges");
        vertexIterator.forEachRemaining(v -> v.addEdge("bla", v));
        graph.commit();
        stopWatch("add edges");

        startWatch("edge iterator");
        Iterator<Edge> edgeIterator = graph.edges();
        stopWatch("edge iterator");

        sw.print();
        System.out.println("-----");
        graph.close();
    }*/

    private void stopWatch(String s) {
        sw.timer(s).stop();
    }

    private void startWatch(String s) {
        sw.timer(s).start();
    }
}
