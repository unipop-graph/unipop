package org.elasticgremlin.elastic;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.*;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticgremlin.ElasticGraphGraphProvider;
import org.elasticgremlin.elasticsearch.ElasticClientFactory;
import org.elasticgremlin.structure.ElasticGraph;
import org.junit.Test;

import java.io.*;
import java.lang.reflect.Method;

public class TemporaryTests {
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
        String path = new java.io.File( "." ).getCanonicalPath() + "\\data";
        File file = new File(path);
        FileUtils.deleteQuietly(file);

        BaseConfiguration config = new BaseConfiguration();
        config.addProperty("elasticsearch.cluster.name", "testgraph");
        String indexName = "graphtest14";
        config.addProperty("elasticsearch.index.name", indexName.toLowerCase());
        config.addProperty("elasticsearch.refresh", true);
        config.addProperty("elasticsearch.client", ElasticClientFactory.ClientType.NODE);
        ElasticGraph graph = new ElasticGraph(config);
        graph.getQueryHandler().clearAllData();
        ElasticGraphGraphProvider elasticGraphProvider = new ElasticGraphGraphProvider();
        Method m = this.getClass().getMethod("testToPassTests");
        LoadGraphWith[] loadGraphWiths = m.getAnnotationsByType(LoadGraphWith.class);
        elasticGraphProvider.loadGraphData(graph, loadGraphWiths[0], this.getClass(), m.getName());
        GraphTraversalSource g = graph.traversal();


        GraphTraversal<Vertex, Vertex> iter = g.V("4").both();
        printTraversalForm(iter);
        //iter.profile().cap(TraversalMetrics.METRICS_KEY);

        System.out.println("iter = " + iter);
        while(iter.hasNext()){
            Object next = iter.next();
            String s = next.toString();
            System.out.println("s = " + s);
        }



        //graph.elasticService.client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
        graph.close();
    }

    public void printTraversalForm(final Traversal traversal) {
        System.out.println("   pre-strategy:" + traversal);
        traversal.hasNext();
        System.out.println("  post-strategy:" + traversal);
    }
}
