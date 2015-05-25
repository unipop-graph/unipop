package org.elasticgremlin.elasticservice;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.elasticsearch.action.admin.cluster.health.*;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.*;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.List;


public class DefaultIndexProvider implements IndexProvider {
    private IndexResult indexResult;
    private MultiIndexResult multiIndexResult;

    @Override
    public void init(Client client, Configuration configuration) throws IOException {
        String indexName = configuration.getString("elasticsearch.index.name", "graph");
        this.multiIndexResult = new MultiIndexResult(new String[]{indexName}, null);
        this.indexResult = new IndexResult(indexName, null);
        createIndex(indexName, client);
    }

    private void createIndex(String indexName, Client client) throws IOException {
        IndicesExistsRequest request = new IndicesExistsRequest(indexName);
        IndicesExistsResponse response = client.admin().indices().exists(request).actionGet();
        if (!response.isExists()) {
            Settings settings = ImmutableSettings.settingsBuilder().put("index.analysis.analyzer.default.type", "keyword").build();
            CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(indexName).setSettings(settings);
            client.admin().indices().create(createIndexRequestBuilder.request()).actionGet();
        }

        final ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest(indexName).timeout(TimeValue.timeValueSeconds(10)).waitForYellowStatus();
        final ClusterHealthResponse clusterHealth = client.admin().cluster().health(clusterHealthRequest).actionGet();
        if (clusterHealth.isTimedOut()) {
            throw new IOException(clusterHealth.getStatus() +
                    " status returned from cluster '" + client.admin().cluster().toString() +
                    "', index '" + indexName + "'");

        }
    }

    @Override
    public IndexResult getIndex(Element element) {
        return indexResult;
    }

    @Override
    public IndexResult getIndex(String label, Object idValue, ElasticService.ElementType elementType) {
        return indexResult;
    }

    @Override
    public MultiIndexResult getIndex(List<HasContainer> hasContainers, ElasticService.ElementType elementType) {
        return multiIndexResult;
    }

    @Override
    public String[] getIndicesForClearGraph() {
        return multiIndexResult.getIndex();
    }


    @Override
    public String toString() {
        return "DefaultIndexProvider{" +
                "indexName='" + indexResult.getIndex() + '\'' +
                '}';
    }
}
