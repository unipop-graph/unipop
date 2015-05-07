package org.apache.tinkerpop.gremlin.elastic.elasticservice;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.elasticsearch.action.admin.cluster.health.*;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.*;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.List;


public class DefaultIndexProvider implements IndexProvider {
    private MutateResult mutateResult;
    private SearchResult searchResult;

    @Override
    public void init(Client client, Configuration configuration) throws IOException {
        String indexName = configuration.getString("elasticsearch.index.name", "graph");
        this.searchResult = new SearchResult(new String[]{indexName}, null);
        this.mutateResult = new MutateResult(indexName, null);
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
    public MutateResult getIndex(String label, Object idValue, ElasticService.ElementType elementType, Object[] keyValues) {
        return mutateResult;
    }

    @Override
    public SearchResult getIndex(List<HasContainer> hasContainers, ElasticService.ElementType elementType) {
        return searchResult;
    }

    @Override
    public String[] getIndicesForClearGraph() {
        return searchResult.getIndex();
    }


    @Override
    public String toString() {
        return "DefaultIndexProvider{" +
                "indexName='" + mutateResult.getIndex() + '\'' +
                '}';
    }
}
