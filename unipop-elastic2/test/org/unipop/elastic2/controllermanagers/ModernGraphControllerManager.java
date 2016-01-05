package org.unipop.elastic2.controllermanagers;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.client.Client;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.controllerprovider.TinkerGraphControllerManager;
import org.unipop.elastic2.controller.edge.ElasticEdgeController;
import org.unipop.elastic2.controller.vertex.ElasticVertexController;
import org.unipop.elastic2.helpers.ElasticClientFactory;
import org.unipop.elastic2.helpers.ElasticHelper;
import org.unipop.elastic2.helpers.ElasticMutations;
import org.unipop.elastic2.helpers.TimingAccessor;
import org.unipop.structure.UniGraph;

import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inject;

public class ModernGraphControllerManager extends TinkerGraphControllerManager {
    private EdgeController edgeController;
    private VertexController vertexController;
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
        edgeController = new ElasticEdgeController(graph, client, elasticMutations, indexName, 0, timing);
        vertexController = new ElasticVertexController(graph, client, elasticMutations, indexName, 0, timing);

        Vertex person = schema.addVertex(T.label, "person", controller, vertexController);
        person.addEdge("knows", person, controller, edgeController);
        Vertex software = schema.addVertex(T.label, "software", controller, vertexController);
        person.addEdge("created", software, controller, edgeController);

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
    protected GraphTraversal<?, VertexController> defaultVertexControllers() {
        return inject(vertexController);
    }

    @Override
    protected GraphTraversal<?, EdgeController> defaultEdgeControllers() {
        return inject(edgeController);
    }

    @Override
    public long edgeCount(Predicates predicates) {
        return 0;
    }

    @Override
    public long edgeCount(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        return 0;
    }

    @Override
    public Map<String, Object> edgeGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        return null;
    }

    @Override
    public Map<String, Object> edgeGroupBy(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        return null;
    }

    @Override
    public long vertexCount(Predicates predicates) {
        return 0;
    }

    @Override
    public Map<String, Object> vertexGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        return null;
    }
}
