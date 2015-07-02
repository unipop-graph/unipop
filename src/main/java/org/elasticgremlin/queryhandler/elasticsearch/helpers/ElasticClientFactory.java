package org.elasticgremlin.queryhandler.elasticsearch.helpers;

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
        String addresses = configuration.getString("elasticsearch.cluster.address", "127.0.0.1:9300");

        if (clientType.equals(ClientType.TRANSPORT_CLIENT)) return createTransportClient(clusterName, addresses);
        else if (clientType.equals(ClientType.NODE_CLIENT)) return createNode(clusterName, true);
        else if (clientType.equals(ClientType.NODE)) return createNode(clusterName, false);
        else throw new IllegalArgumentException("clientType unknown:" + clientType);
    }

    public static TransportClient createTransportClient(String clusterName, String addresses) {
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).put("client.transport.sniff", true).build();
        TransportClient transportClient = new TransportClient(settings);
        for(String address : addresses.split(",")) {
            String[] split = address.split(":");
            if(split.length != 2) throw new IllegalArgumentException("Address invalid:" + address +  ". Should contain ip and port, e.g. 127.0.0.1:9300");
            transportClient.addTransportAddress(new InetSocketTransportAddress(split[0], Integer.parseInt(split[1])));
        }
        return transportClient;
    }

    public static Client createNode(String clusterName, boolean client) {
        Settings settings = NodeBuilder.nodeBuilder().settings().put("script.groovy.sandbox.enabled", true).put("script.disable_dynamic", false).build();
        Node node = NodeBuilder.nodeBuilder().client(client).data(!client).clusterName(clusterName).settings(settings).build();
        node.start();
        return node.client();
    }
}
