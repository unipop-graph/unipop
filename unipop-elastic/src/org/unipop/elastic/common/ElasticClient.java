package org.unipop.elastic.common;

import io.searchbox.action.Action;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.mapping.PutMapping;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticClient {

    private Map<DocumentIdentifier, BulkableAction> bulk;
    String STRING_NOT_ANALYZED = "{\"dynamic_templates\" : [{\"not_analyzed\" : {\"match\" : \"*\",\"match_mapping_type\" : \"string\", \"mapping\" : {\"type\" : \"string\",\"index\" : \"not_analyzed\"}}}]}";

    private final JestClient client;

    public ElasticClient(List<String> addresses) {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder(addresses).multiThreaded(true).build());
        this.client = factory.getObject();
    }

    public void validateIndex(String indexName) {
        try {
            IndicesExists indicesExistsRequest = new IndicesExists.Builder(indexName).build();
            JestResult existsResult = client.execute(indicesExistsRequest);
            if (!existsResult.isSucceeded()) {
                Settings settings = Settings.settingsBuilder().put("index.analysis.analyzer.default.type", "keyword").build();;
                CreateIndex createIndexRequest = new CreateIndex.Builder(indexName).settings(settings).build();
                execute(createIndexRequest);
                //TODO: Make this work. Using the above "keyword" configuration in the meantime.
                PutMapping putMapping = new PutMapping.Builder(indexName, "*", STRING_NOT_ANALYZED).build();
                execute(putMapping);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public JestResult validateNested(String index, String type, String path) {
        PutMapping putMapping = new PutMapping.Builder(
                index,
                type,
                "{ \"" + type + "\" : { \"properties\" : { \"" + path + "\" : {\"type\" : \"nested\"} } } }"
        ).build();

        return execute(putMapping);
    }

    public JestClient getClient() {
        return client;
    }

    public void bulk(BulkableAction action) {
        if(bulk != null && bulk.size() >= 500) refresh();
        if(bulk == null) bulk = new HashMap<>();
        DocumentIdentifier documentIdentifier = new DocumentIdentifier(action.getId(), action.getType(), action.getIndex());
        bulk.put(documentIdentifier, action);
    }

    public void refresh() {
        if(bulk != null) {
            Bulk bulkAction = new Bulk.Builder().addAction(this.bulk.values()).refresh(true).build();
            execute(bulkAction);
            bulk = null;
        }
//        Refresh refresh = new Refresh.Builder().refresh(true).allowNoIndices(true).build();
//        execute(refresh);
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

    class DocumentIdentifier {
        private final String id;
        private final String type;
        private final String index;

        public DocumentIdentifier(String id, String type, String index) {
            this.id = id;
            this.type = type;
            this.index = index;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DocumentIdentifier that = (DocumentIdentifier) o;

            if (!id.equals(that.id)) return false;
            if (!type.equals(that.type)) return false;
            return index.equals(that.index);

        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }
    }
}
