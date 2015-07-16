package org.elasticgremlin;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.*;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.queryhandler.elasticsearch.helpers.*;
import org.elasticgremlin.structure.*;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;

import java.io.*;
import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ElasticGraphGraphProvider extends AbstractGraphProvider {

    private static String CLUSTER_NAME = "test";

    private static final Set<Class> IMPLEMENTATION = new HashSet<Class>() {{
        add(BaseEdge.class);
        add(BaseElement.class);
        add(ElasticGraph.class);
        add(BaseProperty.class);
        add(BaseVertex.class);
        add(BaseVertexProperty.class);
    }};

    private final int port;
    private final Node node;
    private final Client client;

    public ElasticGraphGraphProvider() throws IOException, ExecutionException, InterruptedException {
        System.setProperty("build.dir", System.getProperty("user.dir") + "\\build");

        String path = new java.io.File( "." ).getCanonicalPath() + "\\data";
        File file = new File(path);
        FileUtils.deleteQuietly(file);

        this.port = findFreePort();

        node = ElasticClientFactory.createNode(CLUSTER_NAME, false, port);
        client = node.client();

        final ClusterHealthResponse clusterHealth = client.admin().cluster().prepareHealth().setTimeout(TimeValue.timeValueSeconds(10)).setWaitForGreenStatus().execute().get();
        if (clusterHealth.isTimedOut()) System.out.print(clusterHealth.getStatus());
    }

    private static int findFreePort() {
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(0);
            socket.setReuseAddress(true);
            int port = socket.getLocalPort();
            try {
                socket.close();
            } catch (IOException e) {
                // Ignore IOException on close()
            }
            return port;
        } catch (IOException e) {
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                }
            }
        }
        throw new IllegalStateException("Could not find a free TCP/IP port to start embedded Jetty HTTP Server on");
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, ElasticGraph.class.getName());
            put("elasticsearch.cluster.name", CLUSTER_NAME);
            put("elasticsearch.index.name",graphName.toLowerCase());
            put("elasticsearch.refresh", true);
            put("elasticsearch.client", ElasticClientFactory.ClientType.TRANSPORT_CLIENT.toString());
            put("elasticsearch.cluster.address", "127.0.0.1:" + port);
        }};
    }

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        if (g != null) {
            String indexName = configuration.getString("elasticsearch.index.name");
            ElasticHelper.clearIndex(client, indexName);
            g.close();
        }
        if(g instanceof ElasticGraph)
            ((ElasticGraph)g).getQueryHandler().printStats();
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
