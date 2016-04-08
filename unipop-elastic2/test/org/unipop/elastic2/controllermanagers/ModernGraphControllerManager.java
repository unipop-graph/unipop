package org.unipop.elastic2.controllermanagers;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.client.Client;
import org.unipop.query.UniQuery;
import org.unipop.controllerprovider.TinkerGraphControllerManager;
import org.unipop.elastic2.controller.edge.ElasticEdgeController;
import org.unipop.elastic2.controller.vertex.ElasticVertexController;
import org.unipop.elastic2.helpers.ElasticClientFactory;
import org.unipop.elastic2.helpers.ElasticHelper;
import org.unipop.elastic2.helpers.ElasticMutations;
import org.unipop.elastic2.helpers.TimingAccessor;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniVertex;
import org.unipop.structure.UniVertexProperty;
import org.unipop.structure.UniGraph;

import java.util.List;
import java.util.Map;

public class ModernGraphControllerManager extends TinkerGraphControllerManager {
    private EdgeQueryController edgeQueryController;
    private VertexQueryController vertexQueryController;
    private Client client;
    private ElasticMutations elasticMutations;
    private TimingAccessor timing;

    @Override
    public void init(UniGraph graph, Configuration configuration) throws Exception {
        String indexName = configuration.getString("graphName", "unipop");

        client = ElasticClientFactory.create(configuration);
        ElasticHelper.createIndex(indexName, client);

        timing = new TimingAccessor();
        elasticMutations = new ElasticMutations(false, client, timing);
        edgeQueryController = new ElasticEdgeController(graph, client, elasticMutations, indexName, 0, timing);
        vertexQueryController = new ElasticVertexController(graph, client, elasticMutations, indexName, 0, timing);

        Vertex person = schema.addVertex(T.label, "person", controller, vertexQueryController);
        person.addEdge("knows", person, controller, edgeQueryController);
        Vertex software = schema.addVertex(T.label, "software", controller, vertexQueryController);
        person.addEdge("created", software, controller, edgeQueryController);

    }

    @Override
    public List<UniElement> properties(List<UniElement> elements) {
        throw new NotImplementedException();
    }

    @Override
    public void commit() {
        elasticMutations.commit();
    }

    @Override
    public void close() {
        client.close();
        timing.print();
    }

    @Override
    protected GraphTraversal<?, VertexQueryController> defaultVertexControllers() {
        return inject(vertexQueryController);
    }

    @Override
    protected GraphTraversal<?, EdgeQueryController> defaultEdgeControllers() {
        return inject(edgeQueryController);
    }

    @Override
    public long edgeCount(UniQuery uniQuery) {
        return 0;
    }

    @Override
    public long edgeCount(Vertex[] vertices, Direction direction, String[] edgeLabels, UniQuery uniQuery) {
        return 0;
    }

    @Override
    public Map<String, Object> edgeGroupBy(UniQuery uniQuery, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        return null;
    }

    @Override
    public Map<String, Object> edgeGroupBy(Vertex[] vertices, Direction direction, String[] edgeLabels, UniQuery uniQuery, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        return null;
    }

    @Override
    public long vertexCount(UniQuery uniQuery) {
        return 0;
    }

    @Override
    public Map<String, Object> vertexGroupBy(UniQuery uniQuery, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        return null;
    }

    @Override
    public void addPropertyToVertex(UniVertex vertex, UniVertexProperty vertexProperty) {
        throw new NotImplementedException();
    }

    @Override
    public void removePropertyFromVertex(UniVertex vertex, Property property) {
        throw new NotImplementedException();
    }

    @Override
    public void removeVertex(UniVertex vertex) {
        throw new NotImplementedException();
    }

    @Override
    public List<UniElement> vertexProperties(List<UniVertex> vertices) {
        throw new NotImplementedException();    }

    @Override
    public void update(UniVertex vertex, boolean force) {
        throw new NotImplementedException();
    }

    @Override
    public String getResource() {
        throw new NotImplementedException();
    }
}
