package org.unipop.elastic2.helpers;

import org.apache.commons.configuration.Configuration;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

public class ElasticClientFactory {

    public static class ClientType {
        public static String TRANSPORT_CLIENT = "TRANSPORT_CLIENT";
        public static String NODE_CLIENT = "NODE_CLIENT";
        public static String NODE = "NODE";
    }

    public static Client create(Configuration configuration) throws ExecutionException, InterruptedException, UnknownHostException
    {
        String clientType = configuration.getString("elasticsearch.client", ClientType.NODE);
        String clusterName = configuration.getString("elasticsearch.cluster.name", "elasticsearch");

        if (clientType.equals(ClientType.TRANSPORT_CLIENT)) {
            String concatenatedAddresses = configuration.getString("elasticsearch.cluster.address", "127.0.0.1:9300");
            String[] addresses = concatenatedAddresses.split(",");
            InetSocketTransportAddress[] inetSocketTransportAddresses = new InetSocketTransportAddress[addresses.length];
            for(int i = 0; i < addresses.length; i++) {
                String address = addresses[i];
                String[] split = address.split(":");
                if(split.length != 2) throw new IllegalArgumentException("Address invalid:" + address +  ". Should contain ip and port, e.g. 127.0.0.1:9300");
                inetSocketTransportAddresses[i] = new InetSocketTransportAddress(InetAddress.getByName(split[0]), Integer.parseInt(split[1]));
            }
            return createTransportClient(clusterName, inetSocketTransportAddresses);
        }
        else{
            String port = configuration.getString("elasticsearch.cluster.port", "9300");
            return createNode(clusterName, clientType.equals(ClientType.NODE_CLIENT), Integer.parseInt(port)).client();
        }
    }

    public static TransportClient createTransportClient(String clusterName, InetSocketTransportAddress... addresses) {
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", clusterName)
                .put("client.transport.sniff", true).build();
        TransportClient transportClient = TransportClient.builder().settings(settings).build().addTransportAddresses(addresses);
        return transportClient;
    }

    public static Node createNode(String clusterName, boolean client, int port) throws ExecutionException, InterruptedException {
        if(port == 0) port = findFreePort();
        Settings settings = NodeBuilder.nodeBuilder().settings()
                .put("index.max_result_window", Integer.MAX_VALUE)
                .put("script.groovy.sandbox.enabled", true)
                .put("script.inline", "on")
                .put("script.indexed", "on")
                .put("transport.tcp.port", port).build();
        Node node = NodeBuilder.nodeBuilder().client(client).data(!client).clusterName(clusterName).settings(settings).build();
        node.start();
        final ClusterHealthResponse clusterHealth = node.client().admin().cluster().prepareHealth().setTimeout(TimeValue.timeValueSeconds(10)).setWaitForGreenStatus().execute().get();
        if (clusterHealth.isTimedOut()) System.out.print(clusterHealth.getStatus());
        return node;
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
        throw new IllegalStateException("Could not find free port");
    }
}
