package org.elasticgremlin.elasticservice;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;
import java.util.function.Consumer;

public class ScrollIterator implements Iterator<SearchHit> {

    private SearchResponse scrollResponse;
    private long allowedRemaining;
    private Client client;
    private Iterator<SearchHit> hits;

    public ScrollIterator(SearchRequestBuilder searchRequestBuilder, int scrollSize, long maxSize, Client client) {
        this.client = client;
        this.allowedRemaining = maxSize;
        scrollResponse = searchRequestBuilder
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
    public SearchHit next() {
        allowedRemaining--;
        return hits.next();
    }

    @Override
    public void remove() {
        throw new NotImplementedException();
    }

    @Override
    public void forEachRemaining(Consumer<? super SearchHit> action) {
        hits.forEachRemaining(action);
    }
}
