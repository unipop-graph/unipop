package org.unipop.controllerprovider;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.controller.Predicates;
import org.unipop.controller.elasticsearch.edge.EdgeController;
import org.unipop.controller.elasticsearch.helpers.*;
import org.unipop.controller.elasticsearch.vertex.VertexController;
import org.unipop.structure.*;
import org.elasticsearch.client.Client;

import java.io.IOException;

public class SimpleControllerProvider implements ControllerProvider {

    private EdgeController edgeController;
    private VertexController vertexController;
    private Client client;
    private ElasticMutations elasticMutations;
    private TimingAccessor timing;

    @Override
    public void init(UniGraph graph, Configuration configuration) throws IOException {
        String indexName = configuration.getString("elasticsearch.index.name", "graph");
        boolean refresh = configuration.getBoolean("elasticsearch.refresh", true);
        int scrollSize = configuration.getInt("elasticsearch.scrollSize", 500);
        boolean bulk = configuration.getBoolean("elasticsearch.bulk", false);

        client = ElasticClientFactory.create(configuration);
        ElasticHelper.createIndex(indexName, client);

        timing = new TimingAccessor();
        elasticMutations = new ElasticMutations(bulk, client, timing);
        edgeController = new EdgeController(graph, client, elasticMutations, indexName, scrollSize, refresh, timing);
        vertexController = new VertexController(graph, client, elasticMutations, indexName, scrollSize, refresh, timing);
    }

    @Override
    public void commit() { elasticMutations.commit(); }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public org.unipop.controller.VertexController getVertexHandler(Object[] ids) {
        return vertexController;
    }

    @Override
    public org.unipop.controller.VertexController getVertexHandler(Predicates predicates) {
        return vertexController;
    }

    @Override
    public org.unipop.controller.VertexController getVertexHandler(Object vertexId, String vertexLabel, Edge edge, Direction direction) {
        return vertexController;
    }

    @Override
    public org.unipop.controller.VertexController addVertex(Object id, String label, Object[] properties) {
        return vertexController;
    }

    @Override
    public org.unipop.controller.EdgeController getEdgeHandler(Object[] ids) {
        return edgeController;
    }

    @Override
    public org.unipop.controller.EdgeController getEdgeHandler(Predicates predicates) {
        return edgeController;
    }

    @Override
    public org.unipop.controller.EdgeController getEdgeHandler(Vertex vertex, Direction direction, String[] edgeLabels, Predicates predicates) {
        return edgeController;
    }

    @Override
    public org.unipop.controller.EdgeController addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {
        return edgeController;
    }

    @Override
    public void printStats() {
        timing.print();
    }
}
