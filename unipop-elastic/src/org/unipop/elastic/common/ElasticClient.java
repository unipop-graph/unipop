package org.unipop.elastic.common;

import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.mapping.PutMapping;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElasticClient {
    private final JestClient client;

    public ElasticClient(JSONObject configuration) throws JSONException {
        JSONArray addressesConfiguration = configuration.getJSONArray("addresses");
        List<String> addresses = new ArrayList<>();
        for(int i = 0; i < addressesConfiguration.length(); i++){
            String address = addressesConfiguration.getString(i);
            addresses.add(address);
        }

        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(addresses)
                .multiThreaded(true)
                .build());
        this.client = factory.getObject();
    }

    String NOT_ANALYZED = String.format(String.format("{\"dynamic_templates\" : [{\"not_analyzed\" : {\"match\" : \"*\",\"match_mapping_type\" : \"string\", \"mapping\" : {\"type\" : \"string\",\"index\" : \"not_analyzed\"}}}]}"));


    public void validateIndex(String indexName) {
        try {
            IndicesExists indicesExistsRequest = new IndicesExists.Builder(indexName).build();
            JestResult existsResult = client.execute(indicesExistsRequest);
            if (!existsResult.isSucceeded()) {
                CreateIndex createIndexRequest = new CreateIndex.Builder(indexName).build();
                client.execute(createIndexRequest);

                PutMapping putMapping = new PutMapping.Builder(indexName,"*", NOT_ANALYZED).build();
                client.execute(putMapping);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public JestClient getClient() {
        return client;
    }

    public <T extends JestResult> T execute(Action<T> action) {
        try {
            T result = client.execute(action);
            if (!result.isSucceeded())
                System.out.println(result.getErrorMessage());
            return result;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
