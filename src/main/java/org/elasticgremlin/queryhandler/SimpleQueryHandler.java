package org.elasticgremlin.queryhandler;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.queryhandler.elasticsearch.edgedoc.DocEdgeHandler;
import org.elasticgremlin.queryhandler.elasticsearch.helpers.*;
import org.elasticgremlin.queryhandler.elasticsearch.vertexdoc.DocVertexHandler;
import org.elasticgremlin.structure.*;
import org.elasticsearch.client.Client;

import java.io.IOException;
import java.util.*;

public class SimpleQueryHandler implements QueryHandler {

    private DocEdgeHandler docEdgeHandler;
    private DocVertexHandler elasticDocVertexHandler;
    private Client client;
    private ElasticMutations elasticMutations;
    private TimingAccessor timing;

    @Override
    public void init(ElasticGraph graph, Configuration configuration) throws IOException {
        String indexName = configuration.getString("elasticsearch.index.name", "graph");
        boolean refresh = configuration.getBoolean("elasticsearch.refresh", false);
        int scrollSize = configuration.getInt("elasticsearch.scrollSize", 500);
        boolean bulk = configuration.getBoolean("elasticsearch.bulk", false);

        client = ElasticClientFactory.create(configuration);
        ElasticHelper.createIndex(indexName, client);

        timing = new TimingAccessor();
        elasticMutations = new ElasticMutations(bulk, client, timing);
        docEdgeHandler = new DocEdgeHandler(graph, client, elasticMutations, indexName, scrollSize, refresh, timing);
        elasticDocVertexHandler = new DocVertexHandler(graph, client, elasticMutations, indexName, scrollSize, refresh, timing);
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
    public Map<BaseVertex, List<Edge>> edges(Iterator<BaseVertex> vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        return docEdgeHandler.edges(vertices, direction, edgeLabels, predicates);
    }

    @Override
    public Edge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {
        return docEdgeHandler.addEdge(edgeId, label, outV, inV, properties);
    }

    @Override
    public Iterator<BaseVertex> vertices() {
        return elasticDocVertexHandler.vertices();
    }

    @Override
    public Iterator<BaseVertex> vertices(Object[] vertexIds) {
        return elasticDocVertexHandler.vertices(vertexIds);
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates) {
        return elasticDocVertexHandler.vertices(predicates);
    }

    @Override
    public BaseVertex vertex(Object vertexId, String vertexLabel, Edge edge, Direction direction) {
        return elasticDocVertexHandler.vertex(vertexId, vertexLabel, edge, direction);
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Object[] properties) {
        return elasticDocVertexHandler.addVertex(id, label, properties);
    }

    @Override
    public void printStats() {
        timing.print();
    }
}
