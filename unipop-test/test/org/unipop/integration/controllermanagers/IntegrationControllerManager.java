package org.unipop.integration.controllermanagers;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.client.Client;
import org.unipop.controller.EdgeController;
import org.unipop.controller.VertexController;
import org.unipop.controllerprovider.TinkerGraphControllerManager;
import org.unipop.elastic.controller.edge.ElasticEdgeController;
import org.unipop.elastic.controller.vertex.ElasticVertexController;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.UniGraph;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inject;

public class IntegrationControllerManager extends TinkerGraphControllerManager {
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
        edgeController = new ElasticEdgeController(graph, client, elasticMutations, indexName, 500, true, timing);
        vertexController = new ElasticVertexController(graph, client, elasticMutations, indexName, 500, true, timing);

        Vertex person = schema.addVertex(T.label, "person", controller, vertexController);
        person.addEdge("knows", person, controller, edgeController);
        Vertex software = schema.addVertex(T.label, "software", controller, vertexController);
        person.addEdge("created", software, controller, edgeController);

    }

    @Override
    public void commit() {
        elasticMutations.commit();
        timing.print();
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    protected GraphTraversal<?, VertexController> defaultVertexControllers() {
        return inject(vertexController);
    }

    @Override
    protected GraphTraversal<?, EdgeController> defaultEdgeControllers() {
        return inject(edgeController);
    }
}
