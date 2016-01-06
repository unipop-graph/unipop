package org.unipop.elastic.helpers;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.util.Iterator;
import java.util.Map;

public class QueryIterator<E extends Element> implements Iterator<E> {

    private SearchResponse scrollResponse;
    private long allowedRemaining;
    private final Parser<E> parser;
    private TimingAccessor timing;
    private final int scrollSize;
    private Client client;
    private Iterator<SearchHit> hits;

    public QueryIterator(QueryBuilder query, int scrollSize, long maxSize, Client client,
                         Parser<E> parser, TimingAccessor timing, String... indices) {
        this.scrollSize = scrollSize;
        this.client = client;
        this.allowedRemaining = maxSize;
        this.parser = parser;
        this.timing = timing;

        this.timing.start("query");

        SearchRequestBuilder searchRequestBuilder;

        searchRequestBuilder = client.prepareSearch(indices)
                .setQuery(query);

        if (scrollSize > 0)
            searchRequestBuilder.setScroll(new TimeValue(60000))
                    .setSize(maxSize < scrollSize ? (int) maxSize : scrollSize);
        else searchRequestBuilder.setSize(maxSize < 100000 ? (int) maxSize : 100000);

        this.scrollResponse = searchRequestBuilder.execute().actionGet();

        hits = scrollResponse.getHits().iterator();
        this.timing.stop("query");
    }

    @Override
    public boolean hasNext() {
        if (allowedRemaining <= 0) return false;
        if (hits.hasNext()) return true;

        if (scrollSize > 0) {
            timing.start("scroll");
            scrollResponse = client.prepareSearchScroll(scrollResponse.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
            hits = scrollResponse.getHits().iterator();
            timing.stop("scroll");
        }

        return hits.hasNext();
    }

    @Override
    public E next() {
        allowedRemaining--;
        SearchHit hit = hits.next();
        return parser.parse(hit.id(), hit.getType(), hit.getSource());
    }

    public interface Parser<E> {
        E parse(Object id, String label, Map<String, Object> keyValues);
    }
}
