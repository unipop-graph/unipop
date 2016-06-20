package org.unipop.elastic.common;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.cluster.Health;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.IOException;
import java.util.ArrayList;

public class ElasticHelper {

    public static void createIndex(String indexName, JestClient client) throws IOException {
        CreateIndex createIndexRequest = new CreateIndex.Builder(indexName).build();
        IndicesExists indicesExistsRequest = new IndicesExists.Builder(indexName).build();

        JestResult existsResult = client.execute(indicesExistsRequest);

//        IndicesExistsRequest request = new IndicesExistsRequest(indexName);
//        IndicesExistsResponse response = client.admin().indices().exists(request).actionGet();
        if (!existsResult.isSucceeded()) {
            Settings settings = ImmutableSettings.settingsBuilder()
                    .put("index.analysis.analyzer.default.type", "keyword")
                    .build();
            CreateIndex createIndexWithSettingsRequest = new CreateIndex.Builder(indexName)
                    .settings(settings)
                    .build();
            client.execute(createIndexRequest);

        }

        final JestResult healthResult = client.execute(new Health.Builder().build());

        if (!healthResult.isSucceeded()) {
            throw new IOException(healthResult.getJsonObject().get("status").getAsString() +
                    " status returned from cluster '" + healthResult
                    .getJsonObject()
                    .get("cluster_name")
                    .getAsString() +
                    "', index '" + indexName + "'");

        }
    }

    public static DeleteByQueryResponse clearIndex(Client client, String indexName) {
        DeleteByQueryResponse indexDeleteByQueryResponses = client.prepareDeleteByQuery(indexName).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();

        GetMappingsResponse getMappingsResponse = client.admin().indices().prepareGetMappings(indexName).execute().actionGet();
        ArrayList<String> mappings = new ArrayList();
        getMappingsResponse.getMappings().forEach(map -> {
            map.value.forEach(map2 -> mappings.add(map2.value.type()));
        });

        if (mappings.size() > 0) {
            DeleteMappingResponse deleteMappingResponse = client.admin().indices().prepareDeleteMapping(indexName).setType(mappings.toArray(new String[mappings.size()])).execute().actionGet();
        }

        return indexDeleteByQueryResponses;
    }

    public static void mapNested(Client client, String index, String typeName, String nestedFieldName) {
        try {
            XContentBuilder nestedMapping = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject(typeName)
                    .startObject("properties")
                    .startObject(nestedFieldName)
                    .field("type", "nested")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            client.admin().indices().preparePutMapping(index).setType(typeName).setSource(nestedMapping).execute().actionGet();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public static JestResult deleteIndices(JestClient client) throws IOException {
        return client.execute(new DeleteIndex.Builder("*").build());
    }

    public static DeleteIndexResponse deleteIndices(Client client) {
        return client.admin().indices().delete(new DeleteIndexRequest("*")).actionGet();
    }
}
