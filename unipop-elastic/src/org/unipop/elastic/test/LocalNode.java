package org.unipop.elastic.test;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.File;
import java.io.IOException;

public class LocalNode {
    private final Node node;
    private final Client client;

    public LocalNode(File dataPath) {
        try {
            FileUtils.deleteDirectory(dataPath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Settings.Builder elasticsearchSettings = Settings.settingsBuilder()
                .put("path.data", dataPath)
                .put("path.home", "./data/")
                .put("path.home", "./data/")
                .put("script.inline", "true")
                .put("script.indexed", "true")
                .put("script.update", "true")
                .put("script.groovy.sandbox.enabled", "true");

        this.node = NodeBuilder.nodeBuilder()
                .local(true)
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

    public Node getNode() {
        return node;
    }
}
