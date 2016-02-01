package org.unipop.integration;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.unipop.controllerprovider.ControllerManagerFactory;
import org.unipop.elastic.controller.edge.ElasticEdgeController;
import org.unipop.elastic.controller.star.ElasticStarController;
import org.unipop.elastic.controller.star.inneredge.nested.NestedEdge;
import org.unipop.elastic.controller.star.inneredge.nested.NestedEdgeController;
import org.unipop.elastic.controller.vertex.ElasticVertexController;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.integration.controllermanagers.IntegrationControllerManager;
import org.unipop.manager.JSONSchemaControllerManager;
import org.unipop.process.strategy.SimplifiedStrategyRegistrar;
import org.unipop.structure.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by sbarzilay on 30/01/16.
 */
public class JsonGraphProvider extends AbstractGraphProvider {
    private static String CLUSTER_NAME = "test";

    private static final Set<Class> IMPLEMENTATION = new HashSet<Class>() {{
        add(BaseEdge.class);
        add(BaseElement.class);
        add(UniGraph.class);
        add(BaseProperty.class);
        add(BaseVertex.class);
        add(BaseVertexProperty.class);
    }};

    private final Client client;

    public JsonGraphProvider() throws IOException, ExecutionException, InterruptedException, SQLException, ClassNotFoundException {
        //patch for failing IO tests that wrute to disk
        System.setProperty("build.dir", System.getProperty("user.dir") + "\\build");
        //Delete elasticsearch 'data' directory
        String path = new java.io.File( "." ).getCanonicalPath() + "\\data";
        File file = new File(path);
        FileUtils.deleteQuietly(file);

        Node node = ElasticClientFactory.createNode(CLUSTER_NAME, false, 0);
        client = node.client();

        ElasticHelper.createIndex("person", client);
        ElasticHelper.createIndex("software", client);
        ElasticHelper.createIndex("created", client);
        ElasticHelper.createIndex("knows", client);
        ElasticHelper.createIndex("unipop", client);

        JSONObject obj = new JSONObject();
        List<Map<String, Object>> controllers = new JSONArray();
        Map<String, Object> elasticStar = new JSONObject();
        elasticStar.put("class", ElasticVertexController.class.getCanonicalName());
        elasticStar.put("type", "vertex");
        elasticStar.put("backend", "elastic");
        elasticStar.put("defaultIndex", "unipop");
        elasticStar.put("labels", new JSONArray(){{add("person");}});
        controllers.add(elasticStar);
        Map<String, Object> elasticVertex = new JSONObject();
        elasticVertex.put("class", ElasticVertexController.class.getCanonicalName());
        elasticVertex.put("type", "vertex");
        elasticVertex.put("backend", "elastic");
        elasticVertex.put("defaultIndex", "unipop");
        elasticVertex.put("labels", new JSONArray(){{add("software");}});
        controllers.add(elasticVertex);
        Map<String, Object> elasticEdge = new JSONObject();
        elasticEdge.put("class", ElasticEdgeController.class.getCanonicalName());
        elasticEdge.put("type", "edge");
        elasticEdge.put("backend", "elastic");
        elasticEdge.put("defaultIndex", "unipop");
        elasticEdge.put("labels", new JSONArray(){{add("created");}});
        controllers.add(elasticEdge);
        Map<String, Object> knows = new JSONObject();
        knows.put("class", ElasticEdgeController.class.getCanonicalName());
        knows.put("type", "edge");
        knows.put("backend", "elastic");
        knows.put("defaultIndex", "unipop");
        knows.put("labels", new JSONArray(){{add("knows");}});
        controllers.add(knows);
        obj.put("controllers", controllers);

        FileWriter schema = new FileWriter("schema.json");
        schema.write(obj.toJSONString());
        schema.flush();
        schema.close();
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, UniGraph.class.getName());
            put("graphName",graphName.toLowerCase());
            put("elasticsearch.client", ElasticClientFactory.ClientType.TRANSPORT_CLIENT);
            put("elasticsearch.cluster.name", CLUSTER_NAME);
            put("elasticsearch.cluster.address", "127.0.0.1:" + client.settings().get("transport.tcp.port"));
            put("controllerManagerFactory", (ControllerManagerFactory) JSONSchemaControllerManager::new);
            put("strategyRegistrar", new SimplifiedStrategyRegistrar());
            put("elastic", true);
            put("schema", "schema.json");
        }};
    }

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        if (g != null) {
            String indexName = "created";
            ElasticHelper.clearIndex(client, indexName);
            indexName = "person";
            ElasticHelper.clearIndex(client, indexName);
            indexName = "software";
            ElasticHelper.clearIndex(client, indexName);
            indexName = "knows";
            ElasticHelper.clearIndex(client, indexName);
            indexName = "unipop";
            ElasticHelper.clearIndex(client, indexName);
            g.close();
//            File file = new File("schema.json");
//            FileUtils.deleteQuietly(file);
        }
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATION;
    }

    @Override
    public Object convertId(Object id, Class<? extends Element> c) {
        return id.toString();
    }
}
