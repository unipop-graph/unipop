package org.elasticgremlin.elasticsearch;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;

import java.util.*;
import java.util.function.Function;

public class SearchQuery<E extends Element> implements Iterator<E> {

    private SearchResponse scrollResponse;
    private long allowedRemaining;
    private final Function<SearchHit, E> convertFunc;
    private Client client;
    private Iterator<SearchHit> hits;

    public SearchQuery(String indexName, FilterBuilder filter, int startFrom, int scrollSize, long maxSize, Client client, Function<SearchHit, E> convertFunc, Boolean refresh) {
        this.client = client;
        this.allowedRemaining = maxSize;
        this.convertFunc = convertFunc;

        if (refresh) client.admin().indices().prepareRefresh(indexName).execute().actionGet();
        scrollResponse = client.prepareSearch(indexName)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter))
                .setFrom(startFrom)
                .setScroll(new TimeValue(60000))
                .setSize(maxSize < scrollSize ? (int) maxSize : scrollSize)
                .execute().actionGet();
        hits = scrollResponse.getHits().iterator();
    }

    @Override
    public boolean hasNext() {
        if(allowedRemaining <= 0) return false;
        if(hits.hasNext()) return true;
        
        scrollResponse = client.prepareSearchScroll(scrollResponse.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
        hits = scrollResponse.getHits().iterator();
        return hits.hasNext();
    }

    @Override
    public E next() {
        allowedRemaining--;
        return convertFunc.apply(hits.next());
    }
}
