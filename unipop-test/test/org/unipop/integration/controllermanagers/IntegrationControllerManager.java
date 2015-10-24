package org.unipop.integration.controllermanagers;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
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
import org.unipop.jdbc.controller.star.SqlTableController;
import org.unipop.structure.UniGraph;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class IntegrationControllerManager extends TinkerGraphControllerManager {
    private EdgeController edgeController;
    private VertexController vertexController;
    private Client client;
    private ElasticMutations elasticMutations;
    private TimingAccessor timing;
    private Connection jdbcConnection;

    @Override
    public void init(UniGraph graph, Configuration configuration) throws Exception {
        timing = new TimingAccessor();
        String indexName = configuration.getString("graphName", "unipop");

        client = ElasticClientFactory.create(configuration);
        ElasticHelper.createIndex(indexName, client);

        elasticMutations = new ElasticMutations(false, client, timing);
        edgeController = new ElasticEdgeController(graph, client, elasticMutations, indexName, 500, true, timing);
        vertexController = new ElasticVertexController(graph, client, elasticMutations, indexName, 500, true, timing);

        VertexController personController = vertexController;
        if(configuration.getString("loadGraphWith", "").equals(LoadGraphWith.GraphData.MODERN.toString())){
            Class.forName("org.h2.Driver");
            this.jdbcConnection = DriverManager.getConnection("jdbc:h2:~/test", "sa", "");
            personController = new SqlTableController("PERSON", graph, this.jdbcConnection);
        }

        Vertex person = schema.addVertex(T.label, "person", controller, personController);
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
        try {
            if(jdbcConnection != null)
                jdbcConnection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        timing.print();
    }
//
//    @Override
//    protected GraphTraversal<?, VertexController> defaultVertexControllers() {
//        return inject(vertexController);
//    }
//
//    @Override
//    protected GraphTraversal<?, EdgeController> defaultEdgeControllers() {
//        return inject(edgeController);
//    }
}
