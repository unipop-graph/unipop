package org.unipop.elastic.basic;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.client.Client;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.controllerprovider.ControllerProvider;
import org.unipop.elastic.controller.edge.ElasticEdgeController;
import org.unipop.elastic.controller.vertex.ElasticVertexController;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.io.IOException;
import java.util.Iterator;

public class BasicControllerProvider implements ControllerProvider {

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
        edgeController = new ElasticEdgeController(graph, client, elasticMutations, indexName, scrollSize, refresh, timing);
        vertexController = new ElasticVertexController(graph, client, elasticMutations, indexName, scrollSize, refresh, timing);
    }

    @Override
    public void commit() { elasticMutations.commit(); }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public void printStats() {
        timing.print();
    }

    @Override
    public Iterator<Edge> edges(Object[] ids) {
        return edgeController.edges(ids);
    }

    @Override
    public Iterator<Edge> edges(Predicates predicates, MutableMetrics metrics) {
        return edgeController.edges(predicates, metrics);
    }

    @Override
    public Iterator<Edge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, MutableMetrics metrics) {
        return edgeController.edges(vertices, direction, edgeLabels, predicates, metrics);
    }

    @Override
    public Edge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {
        return edgeController.addEdge(edgeId, label, outV, inV, properties);
    }

    @Override
    public Iterator<Vertex> vertices(Object[] ids) {
        return vertexController.vertices(ids);
    }

    @Override
    public Iterator<Vertex> vertices(Predicates predicates, MutableMetrics metrics) {
        return vertexController.vertices(predicates, metrics);
    }

    @Override
    public BaseVertex vertex(Edge edge, Direction direction, Object vertexId, String vertexLabel) {
        return vertexController.vertex(edge, direction, vertexId, vertexLabel);
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Object[] properties) {
        return vertexController.addVertex(id, label, properties);
    }
}
