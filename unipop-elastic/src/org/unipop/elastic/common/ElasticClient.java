package org.unipop.elastic.common;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.ClearScrollRequest;
import co.elastic.clients.elasticsearch.core.ScrollRequest;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticClient {

    private final static Logger logger = LoggerFactory.getLogger(ElasticClient.class);

    /** Dynamic template that maps every string field as a non-analyzed keyword (ES 8 form). */
    private static final String KEYWORD_DYNAMIC_TEMPLATE =
            "{\"dynamic_templates\":[{\"strings_as_keyword\":{\"match_mapping_type\":\"string\"," +
            "\"mapping\":{\"type\":\"keyword\"}}}]}";

    private final RestClient restClient;
    private final ElasticsearchTransport transport;
    private final ElasticsearchClient client;

    /** Pending bulk operations keyed by a per-document identity, flushed on refresh() or at 500. */
    private Map<DocumentIdentifier, BulkOperation> bulk;

    public ElasticClient(List<String> addresses) {
        HttpHost[] hosts = addresses.stream().map(HttpHost::create).toArray(HttpHost[]::new);
        this.restClient = RestClient.builder(hosts).build();
        this.transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        this.client = new ElasticsearchClient(transport);
    }

    public ElasticsearchClient getClient() {
        return client;
    }

    public void validateIndex(String indexName) {
        try {
            boolean exists = client.indices().exists(e -> e.index(indexName)).value();
            if (!exists) {
                CreateIndexRequest request = CreateIndexRequest.of(c -> c
                        .index(indexName)
                        .mappings(m -> m.withJson(new StringReader(KEYWORD_DYNAMIC_TEMPLATE))));
                client.indices().create(request);
                logger.info("created index {}", indexName);
            }
        } catch (IOException e) {
            logger.error("failed to validate/create index {}", indexName, e);
        }
    }

    public void bulk(Element element, BulkOperation op) {
        if (bulk != null && bulk.size() >= 500) refresh();
        if (bulk == null) bulk = new HashMap<>();
        bulk.put(new DocumentIdentifier(element, op), op);
    }

    public void refresh() {
        if (bulk == null || bulk.isEmpty()) {
            bulk = null;
            return;
        }
        List<BulkOperation> ops = new ArrayList<>(bulk.values());
        try {
            BulkResponse response = client.bulk(BulkRequest.of(b -> b.operations(ops).refresh(Refresh.True)));
            if (response.errors()) {
                response.items().forEach(item -> {
                    if (item.error() != null) logger.error("bulk item error: {}", item.error().reason());
                });
            }
        } catch (IOException e) {
            logger.error("failed executing bulk", e);
        } finally {
            bulk = null;
        }
    }

    public SearchResponse<Map> search(SearchRequest request) {
        try {
            return client.search(request, Map.class);
        } catch (IOException e) {
            logger.error("failed executing search: {}", request, e);
            return null;
        }
    }

    public ScrollResponse<Map> scroll(String scrollId) {
        try {
            ScrollRequest request = ScrollRequest.of(s -> s.scrollId(scrollId).scroll(Time.of(t -> t.time("6m"))));
            return client.scroll(request, Map.class);
        } catch (IOException e) {
            logger.error("failed executing scroll", e);
            return null;
        }
    }

    public void clearScroll(String scrollId) {
        try {
            client.clearScroll(ClearScrollRequest.of(c -> c.scrollId(scrollId)));
        } catch (IOException e) {
            logger.warn("failed clearing scroll", e);
        }
    }

    public void close() {
        try {
            transport.close();
            restClient.close();
        } catch (IOException e) {
            logger.warn("failed closing client", e);
        }
    }

    /** Identity for de-duplicating pending bulk ops for the same logical document. */
    public static class DocumentIdentifier {
        private final Element element;
        private final int opHash;

        public DocumentIdentifier(Element element, BulkOperation op) {
            this.element = element;
            this.opHash = op.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DocumentIdentifier that = (DocumentIdentifier) o;
            return opHash == that.opHash && element.equals(that.element);
        }

        @Override
        public int hashCode() {
            return 31 * element.id().hashCode() + opHash;
        }
    }
}
