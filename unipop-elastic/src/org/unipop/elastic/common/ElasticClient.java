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
import io.searchbox.indices.Refresh;
import io.searchbox.indices.mapping.PutMapping;
import io.searchbox.params.Parameters;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ElasticClient {

    private Map<DocumentIdentifier, BulkableAction> bulk;
    private final JestClient client;


    private final String STRING_NOT_ANALYZED = "{\"dynamic_templates\" : [{\"not_analyzed\" : {\"match\" : \"*\",\"match_mapping_type\" : \"string\", \"mapping\" : {\"type\" : \"string\",\"index\" : \"not_analyzed\"}}}]}";
    private static final Logger logger = LoggerFactory.getLogger(ElasticClient.class);

    public ElasticClient(List<String> addresses) {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder(addresses).multiThreaded(true).build());
        this.client = factory.getObject();
    }

    public void validateIndex(Iterator<String> indices) {
        logger.debug("validating indices, indices: {}", indices);
        indices.forEachRemaining(indexName -> {
            try {
                IndicesExists indicesExistsRequest = new IndicesExists.Builder(indexName).build();
                logger.debug("validating index exists, indexName: {}, indicesExistsRequest: {}", indexName, indicesExistsRequest);
                JestResult existsResult = client.execute(indicesExistsRequest);
                logger.debug("executed existence validation, result: {}", existsResult);
                if (!existsResult.isSucceeded()) {
                    Settings settings = Settings.settingsBuilder().put("index.analysis.analyzer.default.type", "keyword").build();;
                    logger.debug("index does not exist, creating index with settings: {}", settings);
                    CreateIndex createIndexRequest = new CreateIndex.Builder(indexName).settings(settings).build();
                    logger.debug("formed createIndexRequest, executing. request: {}", createIndexRequest);
                    execute(createIndexRequest);
                    //TODO: Make this work. Using the above "keyword" configuration in the meantime.
                    PutMapping putMapping = new PutMapping.Builder(indexName, "*", STRING_NOT_ANALYZED).build();
                    logger.debug("executing putMapping action. request: {}", putMapping);
                    execute(putMapping);
                }
            } catch (IOException e) {
                logger.error("connection failed to elasticsearch", e);
            }
        });
        logger.info("completed validating indices. refreshing.");
        refresh();
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
        if(this.bulk != null) {
            logger.debug("flushing bulk and executing it, bulk: {}", this.bulk);
            Bulk bulkAction = new Bulk.Builder().addAction(this.bulk.values()).refresh(true).build();
            logger.debug("formed bulkAction, executing it. bulkAction: {}", bulkAction);
            execute(bulkAction);
            this.bulk = null;
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
