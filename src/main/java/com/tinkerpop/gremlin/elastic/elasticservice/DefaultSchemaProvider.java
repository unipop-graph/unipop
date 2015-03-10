package com.tinkerpop.gremlin.elastic.elasticservice;

import com.tinkerpop.gremlin.elastic.structure.ElasticElement;
import com.tinkerpop.gremlin.structure.Element;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.ArrayUtils;
import org.elasticsearch.action.admin.cluster.health.*;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.*;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;

import java.io.IOException;


public class DefaultSchemaProvider implements SchemaProvider {
    public static String TYPE = "ty";
    String indexName;
    private Client client;

    @Override
    public void init(Client client, Configuration configuration) throws IOException {
        this.client = client;
        indexName = configuration.getString("elasticsearch.index.name", "graph");
        createIndex();
    }

    private void createIndex() throws IOException {
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

    public void clearAllData() {
        client.prepareDeleteByQuery(indexName).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
    }

    @Override
    public void close() {

    }

    @Override
    public String toString() {
        return "DefaultSchemaProvider{" +
                "indexName='" + indexName + '\'' +
                '}';
    }

    @Override
    public AddElementResult addElement(String label, Object idValue, ElasticElement.Type type, Object[] keyValues) {
        Object[] all = ArrayUtils.addAll(keyValues, TYPE, type.toString());
        return new AddElementResult() {
            @Override
            public String getIndex() {
                return indexName;
            }

            @Override
            public Object[] getKeyValues() {
                return all;
            }
        };
    }

    @Override
    public String getIndex(Element element) {
        return indexName;
    }

    @Override
    public String getIndex(Object id) {
        return indexName;
    }

    @Override
    public SearchResult search(FilterBuilder filter, ElasticElement.Type type, String[] labels) {
        return new SearchResult() {
            @Override
            public String[] getIndices() {
                return new String[]{indexName};
            }

            @Override
            public FilterBuilder getFilter() {
                FilterBuilder finalFilter = FilterBuilders.termFilter(TYPE, type.toString());
                if (filter != null) finalFilter = FilterBuilders.andFilter(finalFilter, filter);
                return finalFilter;
            }
        };

    }


}
