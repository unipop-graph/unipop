package org.elasticgremlin.elasticservice;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;
import java.util.function.Consumer;

/**
 * Created by Sean on 5/26/2015.
 */
public class ScrollIterator implements Iterator<SearchHit> {

    private SearchResponse scrollResponse;
    private Client client;
    private SearchHit[] hits;
    private int currentIndex;
    private int count;

    public ScrollIterator(SearchRequestBuilder searchRequestBuilder, Client client) {
        scrollResponse = searchRequestBuilder
                .setScroll(new TimeValue(60000))
                .setSize(100).execute().actionGet(); // 100 elements per shard per scroll
        scrollResponse = client.prepareSearchScroll(scrollResponse.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
        this.client = client;
        hits = scrollResponse.getHits().getHits();
        currentIndex = 0;
        count = 0;
    }

    @Override
    public boolean hasNext() {
        return currentIndex < hits.length ? true : scrollResponse.getHits().getTotalHits() < count;
    }

    @Override
    public SearchHit next() {
        if (currentIndex < hits.length) {
            count++;
            return hits[currentIndex++];
        }
        else{
            if (scrollResponse.getHits().getTotalHits() < count){
                currentIndex = 0;
                scrollResponse = client.prepareSearchScroll(scrollResponse.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
                hits = scrollResponse.getHits().getHits();
                count++;
                return hits[currentIndex++];
            }
        }
        throw new ArrayIndexOutOfBoundsException(count);
    }

    @Override
    public void remove() {
        throw new NotImplementedException();
    }

    @Override
    public void forEachRemaining(Consumer<? super SearchHit> action) {
        while (hasNext()){
            action.accept(next());
        }
    }
}
