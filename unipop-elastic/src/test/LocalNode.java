package test;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

public class LocalNode {
    private final Client client;
    private final EmbeddedElastic elastic;

    public LocalNode(File dataPath) throws NodeValidationException, IOException, InterruptedException {
        try {
            FileUtils.deleteDirectory(dataPath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.elastic = EmbeddedElastic.builder()
                .withElasticVersion("5.3.1")
                .withSetting("path.data", dataPath)
                .withSetting("path.home", "./data/")
//                .withSetting("index.number_of_shards", "1")
//                .withSetting("index.number_of_replicas", "0")
//                .withSetting("discovery.zen.ping.multicast.enabled", "false")
//                .withSetting("script.inline", "true")
//                .withSetting("script.allowed_types", "inline")
//                .withSetting("script.allowed_contexts", "search, update")
                .withSetting("script.engine.groovy.inline", "true")
//                .withSetting("script.indexed", "true")
//                .withSetting("script.update", "true")
//                .withSetting("script.groovy.sandbox.enabled", "true")
                .withSetting(PopularProperties.TRANSPORT_TCP_PORT, 9350)
                .withSetting(PopularProperties.CLUSTER_NAME, "unipop")
                .build();

        elastic.start();


        this.client = new PreBuiltTransportClient(Settings.builder().put("cluster.name", "unipop").build())
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9350));

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
