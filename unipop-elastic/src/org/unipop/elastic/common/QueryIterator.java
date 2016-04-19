package org.unipop.elastic.common;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.Iterator;

public class QueryIterator<E extends Element> implements Iterator<E> {

    private SearchResponse scrollResponse;
    private final Parser<E> parser;
    private final int scrollSize;
    private Client client;
    private Iterator<SearchHit> hits;

    public QueryIterator(QueryBuilder query, int scrollSize, int maxSize, Client client,
                         Parser<E> parser, String... indices) {
        this.scrollSize = scrollSize;
        this.client = client;
        this.parser = parser;

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indices).setQuery(query);

        if (scrollSize > 0)
            searchRequestBuilder.setScroll(new TimeValue(60000))
                    .setSize(maxSize < scrollSize ?  maxSize : scrollSize);
        else searchRequestBuilder.setSize(maxSize < 100000 ?  maxSize : 100000);

        this.scrollResponse = searchRequestBuilder.execute().actionGet();

        hits = scrollResponse.getHits().iterator();
    }

    @Override
    public boolean hasNext() {
        if (hits.hasNext()) return true;

        if (scrollSize > 0) {
            scrollResponse = client.prepareSearchScroll(scrollResponse.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
            hits = scrollResponse.getHits().iterator();
        }

        return hits.hasNext();
    }

    @Override
    public E next() {
        SearchHit hit = hits.next();
        return parser.parse(hit);
    }

    public interface Parser<E> {
        E parse(SearchHit hit);
    }
}
