package org.elasticgremlin.queryhandler;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.queryhandler.elasticsearch.edgedoc.DocEdgeHandler;
import org.elasticgremlin.queryhandler.elasticsearch.helpers.*;
import org.elasticgremlin.queryhandler.elasticsearch.vertexdoc.DocVertexHandler;
import org.elasticgremlin.structure.ElasticGraph;
import org.elasticsearch.client.Client;

import java.io.IOException;
import java.util.Iterator;

public class SimpleQueryHandler implements QueryHandler {

    private DocEdgeHandler docEdgeHandler;
    private DocVertexHandler elasticDocVertexHandler;
    private Client client;
    private ElasticMutations elasticMutations;

    @Override
    public void init(ElasticGraph graph, Configuration configuration) throws IOException {
        String indexName = configuration.getString("elasticsearch.index.name", "graph");
        boolean refresh = configuration.getBoolean("elasticsearch.refresh", false);
        int scrollSize = configuration.getInt("elasticsearch.scrollSize", 500);
        boolean bulk = configuration.getBoolean("elasticsearch.bulk", false);

        client = ElasticClientFactory.create(configuration);
        ElasticHelper.createIndex(indexName, client);
        elasticMutations = new ElasticMutations(bulk, client);
        docEdgeHandler = new DocEdgeHandler(graph, client, elasticMutations, indexName, scrollSize, refresh);
        elasticDocVertexHandler = new DocVertexHandler(graph, client, elasticMutations, indexName, scrollSize, refresh);
    }

    @Override
    public void commit() { elasticMutations.commit(); }
    @Override
    public void close() {
        client.close();
    }

    @Override
    public Iterator<Edge> edges() {
        return docEdgeHandler.edges();
    }

    @Override
    public Iterator<Edge> edges(Object[] edgeIds) {
        return docEdgeHandler.edges(edgeIds);
    }

    @Override
    public Iterator<Edge> edges(Predicates predicates) {
        return docEdgeHandler.edges(predicates);
    }

    @Override
    public Iterator<Edge> edges(Vertex vertex, Direction direction, String[] edgeLabels, Predicates predicates) {
        return docEdgeHandler.edges(vertex, direction, edgeLabels, predicates);
    }

    @Override
    public Edge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {
        return docEdgeHandler.addEdge(edgeId, label, outV, inV, properties);
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

}
