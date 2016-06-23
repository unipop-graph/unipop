package org.unipop.elastic.common;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.File;

public class ElasticNode {
    private final Node node;
    private final Client client;

    public ElasticNode(File dataPath, String clusterName) {
        Settings.Builder elasticsearchSettings = Settings.settingsBuilder()
                .put("path.data", dataPath)
                .put("script.groovy.sandbox.enabled", true)
                .put("path.home", "./data/");

        this.node = NodeBuilder.nodeBuilder()
                .local(true)
                .data(true)
                .client(false)
                .clusterName(clusterName)
                .settings(elasticsearchSettings.build())
                .node();


        this.client = node.client();
        checkHealth();
    }

    public void checkHealth() {
        final ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest().timeout(TimeValue.timeValueSeconds(10)).waitForYellowStatus();
        final ClusterHealthResponse clusterHealth = client.admin().cluster().health(clusterHealthRequest).actionGet();
        if (clusterHealth.isTimedOut()) {
            System.out.println(clusterHealth.getStatus() +
                    " status returned from cluster '" + client.admin().cluster().toString());

        }
    }

    public DeleteIndexResponse deleteIndices() {
        return client.admin().indices().prepareDelete("*").execute().actionGet();
    }

    public Client getClient() {
        return client;
    }
}
