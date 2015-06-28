package org.elasticgremlin.elasticsearch;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class QueryIterator<E extends Element> implements Iterator<E> {

    private SearchResponse scrollResponse;
    private long allowedRemaining;
    private final Function<SearchHit, E> convertFunc;
    private final Consumer<List<E>> siblingsFunc;
    private Client client;
    private Iterator<E> hits;

    public QueryIterator(FilterBuilder filter, int startFrom, int scrollSize, long maxSize, Client client,
                         Function<SearchHit, E> convertFunc, Consumer<List<E>> siblingsFunc,
                         Boolean refresh, String... indices) {
        this.client = client;
        this.allowedRemaining = maxSize;
        this.convertFunc = convertFunc;
        this.siblingsFunc = siblingsFunc;

        if (refresh) client.admin().indices().prepareRefresh(indices).execute().actionGet();
        scrollResponse = client.prepareSearch(indices)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter))
                .setFrom(startFrom)
                .setScroll(new TimeValue(60000))
                .setSize(maxSize < scrollSize ? (int) maxSize : scrollSize)
                .execute().actionGet();

        getNextHits();
    }

    @Override
    public boolean hasNext() {
        if(allowedRemaining <= 0) return false;
        if(hits.hasNext()) return true;
        
        scrollResponse = client.prepareSearchScroll(scrollResponse.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
        getNextHits();
        return hits.hasNext();
    }

    @Override
    public E next() {
        allowedRemaining--;
        return hits.next();
    }

    private void getNextHits() {
        List<E> nextHits = new ArrayList<>();
        scrollResponse.getHits().iterator().forEachRemaining(hit -> nextHits.add(convertFunc.apply(hit)));
        if (siblingsFunc != null) {
            siblingsFunc.accept(nextHits);
        }
        hits = nextHits.iterator();
    }
}
