package org.elasticgremlin.elasticsearch;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.elasticsearch.edge.ElasticDocEdgeHandler;
import org.elasticgremlin.elasticsearch.vertex.ElasticDocVertexHandler;
import org.elasticgremlin.querying.*;
import org.elasticgremlin.structure.ElasticGraph;
import org.elasticsearch.client.Client;

import java.io.IOException;
import java.util.Iterator;

public class SimpleQueryHandler implements QueryHandler {
    private final ElasticDocEdgeHandler elasticDocEdgeHandler;
    private final ElasticDocVertexHandler elasticDocVertexHandler;
    private final Client client;
    private final String indexName;
    private final ElasticMutations elasticMutations;
    private final boolean refresh;
    private final int scrollSize;


    public SimpleQueryHandler(ElasticGraph graph, Configuration configuration) throws IOException {
        indexName = configuration.getString("elasticsearch.index.name", "graph");
        this.refresh = configuration.getBoolean("elasticsearch.refresh", false);
        this.scrollSize = configuration.getInt("elasticsearch.scrollSize", 100);

        client = ElasticClientFactory.create(configuration);
        ElasticHelper.createIndex(indexName, client);
        elasticMutations = new ElasticMutations(configuration, client);
        elasticDocEdgeHandler = new ElasticDocEdgeHandler(graph, client, elasticMutations, indexName, scrollSize, refresh);
        elasticDocVertexHandler = new ElasticDocVertexHandler(graph, client, elasticMutations, indexName, scrollSize, refresh);
    }

    @Override
    public Iterator<Edge> edges() {
        return elasticDocEdgeHandler.edges();
    }

    @Override
    public Iterator<Edge> edges(Object[] edgeIds) {
        return elasticDocEdgeHandler.edges(edgeIds);
    }

    @Override
    public Iterator<Edge> edges(Predicates predicates) {
        return elasticDocEdgeHandler.edges(predicates);
    }

    @Override
    public Iterator<Edge> edges(Vertex vertex, Direction direction, String[] edgeLabels, Predicates predicates) {
        return elasticDocEdgeHandler.edges(vertex, direction, edgeLabels, predicates);
    }

    @Override
    public Edge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {
        return elasticDocEdgeHandler.addEdge(edgeId, label, outV, inV, properties);
    }

    @Override
    public Iterator<Vertex> vertices() {
        return elasticDocVertexHandler.vertices();
    }

    @Override
    public Iterator<Vertex> vertices(Object[] vertexIds) {
        return elasticDocVertexHandler.vertices(vertexIds);
    }

    @Override
    public Iterator<Vertex> vertices(Predicates predicates) {
        return elasticDocVertexHandler.vertices(predicates);
    }

    @Override
    public Vertex vertex(Object vertexId, String vertexLabel, Edge edge, Direction direction) {
        return elasticDocVertexHandler.vertex(vertexId, vertexLabel, edge, direction);
    }

    @Override
    public Vertex addVertex(Object id, String label, Object[] properties) {
        return elasticDocVertexHandler.addVertex(id, label, properties);
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public void clearAllData() {
        elasticMutations.clearAllData(new String[]{indexName});
    }
}
