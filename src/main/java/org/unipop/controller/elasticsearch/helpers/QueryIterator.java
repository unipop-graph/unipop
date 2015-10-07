package org.unipop.controller.elasticsearch.helpers;

import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;

import java.util.*;
import java.util.function.Function;

public class QueryIterator<E extends Element> implements Iterator<E> {

    private SearchResponse scrollResponse;
    private long allowedRemaining;
    private final Function<SearchHit,? extends E> convertFunc;
    private TimingAccessor timing;
    private Client client;
    private Iterator<SearchHit> hits;

    public QueryIterator(FilterBuilder filter, int startFrom, int scrollSize, long maxSize, Client client,
                         Function<SearchHit,? extends E> convertFunc,
                         Boolean refresh, TimingAccessor timing, String... indices) {
        this.client = client;
        this.allowedRemaining = maxSize;
        this.convertFunc = convertFunc;
        this.timing = timing;

        if (refresh) client.admin().indices().prepareRefresh(indices).execute().actionGet();
        this.timing.start("query");
        scrollResponse = client.prepareSearch(indices)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter))
                .setFrom(startFrom)
                .setScroll(new TimeValue(60000))
                .setSize(maxSize < scrollSize ? (int) maxSize : scrollSize)
                .execute().actionGet();

        hits = scrollResponse.getHits().iterator();
        this.timing.stop("query");
    }

    @Override
    public boolean hasNext() {
        if(allowedRemaining <= 0) return false;
        if(hits.hasNext()) return true;

        timing.start("scroll");
        scrollResponse = client.prepareSearchScroll(scrollResponse.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
        hits = scrollResponse.getHits().iterator();
        timing.stop("scroll");

        return hits.hasNext();
    }

    @Override
    public E next() {
        allowedRemaining--;
        return convertFunc.apply(hits.next());
    }
}
