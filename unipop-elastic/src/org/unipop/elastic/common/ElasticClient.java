package org.unipop.elastic.common;

import com.google.gson.Gson;
import io.searchbox.action.Action;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.Refresh;
import io.searchbox.indices.mapping.PutMapping;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElasticClient {

    Gson gson = new Gson();
    private final static Logger logger = LoggerFactory.getLogger(ElasticClient.class);

    private List<BulkableAction> bulk;
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
            logger.debug("created indexExistsRequests: {}", indicesExistsRequest);
            JestResult existsResult = client.execute(indicesExistsRequest);
            logger.debug("indexExistsRequests result: {}", existsResult);
            if (!existsResult.isSucceeded()) {
                Settings settings = Settings.settingsBuilder().put("index.analysis.analyzer.default.type", "keyword").build();
                CreateIndex createIndexRequest = new CreateIndex.Builder(indexName).settings(settings).build();
                execute(createIndexRequest);
                //TODO: Make this work. Using the above "keyword" configuration in the meantime.
                PutMapping putMapping = new PutMapping.Builder(indexName, "*", STRING_NOT_ANALYZED).build();
                execute(putMapping);
                logger.info("created index with settings: {}, indexName: {}, putMapping: {}", settings, indexName, putMapping);
            }
        } catch (IOException e) {
            logger.error("failed to connect to elastic cluster", e);
        }
    }

    public JestResult validateNested(String index, String type, String path) {
        PutMapping putMapping = new PutMapping.Builder(
                index,
                type,
                "{ \"" + type + "\" : { \"properties\" : { \"" + path + "\" : {\"type\" : \"nested\"} } } }"
        ).build();
        logger.info("putting mapping for nested, mapping: {}", putMapping);
        return execute(putMapping);
    }

    public void bulk(BulkableAction action) {
        if(bulk != null && bulk.size() >= 500) refresh();
        if(bulk == null) bulk = new ArrayList<>();
        bulk.add(action);    }

    public void refresh() {
        if(bulk != null) {
            Bulk bulkAction = new Bulk.Builder().addAction(this.bulk).refresh(true).build();
            JestResult res = execute(bulkAction);
            bulk = null;
        }
//        Refresh refresh = new Refresh.Builder().refresh(true).allowNoIndices(true).build();
//        execute(refresh);
    }

    public <T extends JestResult> T execute(Action<T> action) {
        try {
            logger.debug("executing action: {}, payload: {}", action, action.getData(gson));
            T result = client.execute(action);
            if (!result.isSucceeded())
                logger.warn(result.getErrorMessage());
            return result;
        } catch (IOException e) {
            logger.error("failed executing error: {}, action: {}, payload: {}", e, action, action.getData(gson));
            return null;
        }
    }

    public void close() {
        logger.info("shutting down client, client: {}", client);
        client.shutdownClient();
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
