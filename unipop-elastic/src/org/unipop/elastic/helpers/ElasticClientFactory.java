package org.unipop.elastic.helpers;

import org.apache.commons.configuration.Configuration;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.*;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.*;

public class ElasticClientFactory {

    public static class ClientType {
        public static String TRANSPORT_CLIENT = "TRANSPORT_CLIENT";
        public static String NODE_CLIENT = "NODE_CLIENT";
        public static String NODE = "NODE";
    }

    public static Client create(Configuration configuration) {
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
                inetSocketTransportAddresses[i] = new InetSocketTransportAddress(split[0], Integer.parseInt(split[1]));
            }
            return createTransportClient(clusterName, inetSocketTransportAddresses);
        }
        else{
            String port = configuration.getString("elasticsearch.cluster.port", "9300");
            return createNode(clusterName, clientType.equals(ClientType.NODE_CLIENT), Integer.parseInt(port)).client();
        }
    }

    public static TransportClient createTransportClient(String clusterName, InetSocketTransportAddress... addresses) {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", clusterName)
                .put("client.transport.sniff", true).build();
        TransportClient transportClient = new TransportClient(settings).addTransportAddresses(addresses);
        return transportClient;
    }

    public static Node createNode(String clusterName, boolean client, int port) {
        Settings settings = NodeBuilder.nodeBuilder().settings()
                .put("script.groovy.sandbox.enabled", true)
                .put("script.disable_dynamic", false)
                .put("transport.tcp.port", port).build();
        Node node = NodeBuilder.nodeBuilder().client(client).data(!client).clusterName(clusterName).settings(settings).build();
        node.start();
        return node;
    }
}
