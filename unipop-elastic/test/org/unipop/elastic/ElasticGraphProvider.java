package org.unipop.elastic;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.unipop.controllerprovider.ControllerManagerFactory;
import org.unipop.elastic.controllermanagers.BasicElasticControllerManager;
import org.unipop.elastic.controllermanagers.ElasticStarControllerManager;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.process.strategy.DefaultStrategyRegistrar;
import org.unipop.process.strategy.SimplifiedStrategyRegistrar;
import org.unipop.structure.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class ElasticGraphProvider extends AbstractGraphProvider {

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

    public ElasticGraphProvider() throws IOException, ExecutionException, InterruptedException {
        //patch for failing IO tests that wrute to disk
        System.setProperty("build.dir", System.getProperty("user.dir") + "\\build");
        //Delete elasticsearch 'data' directory
        String path = new java.io.File( "." ).getCanonicalPath() + "\\data";
        File file = new File(path);
        FileUtils.deleteQuietly(file);

        Node node = ElasticClientFactory.createNode(CLUSTER_NAME, false, 0);
        client = node.client();
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, UniGraph.class.getName());
            put("graphName",graphName.toLowerCase());
            put("elasticsearch.client", ElasticClientFactory.ClientType.TRANSPORT_CLIENT);
            put("elasticsearch.cluster.name", CLUSTER_NAME);
            put("elasticsearch.cluster.address", "127.0.0.1:" + client.settings().get("transport.tcp.port"));

            put("controllerManagerFactory", (ControllerManagerFactory)() -> new BasicElasticControllerManager());
//            put("controllerManagerFactory", (ControllerManagerFactory)() -> new ElasticStarControllerManager());
            //put("controllerManagerFactory", (ControllerManagerFactory)() -> new ModernGraphControllerManager());

//            put("strategyRegistrar", new SimplifiedStrategyRegistrar());
        }};
    }

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        if (g != null) {
            String indexName = configuration.getString("graphName");
            ElasticHelper.clearIndex(client, indexName);
            g.close();
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

    public Client getClient() {
        return client;
    }
}
