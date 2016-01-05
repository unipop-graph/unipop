package org.unipop.elastic2.controller.schema.helpers;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by Roman on 3/16/2015.
 */
public class SearchHitScrollIterable implements Iterable<SearchHit> {
    //region Constructor
    public SearchHitScrollIterable(ElasticGraphConfiguration configuration,
                                   SearchRequestBuilder searchRequestBuilder,
                                   long limit,
                                   Client client) {
        this.configuration = configuration;
        this.searchRequestBuilder = searchRequestBuilder;
        this.limit = limit;
        this.client = client;
    }
    //endregion

    //region Iterable Implementation
    @Override
    public Iterator<SearchHit> iterator() {
        return new ScrollIterator(this);
    }
    //endregion

    //region Properties
    protected SearchRequestBuilder getSearchRequestBuilder() {
        return this.searchRequestBuilder;
    }

    protected Client getClient() {
        return this.client;
    }

    protected long getLimit() {
        return this.limit;
    }
    //endregion

    public ElasticGraphConfiguration getElasticGraphConfiguration() {
        return this.configuration;
    }

    //region Fields
    private ElasticGraphConfiguration configuration;
    private SearchRequestBuilder searchRequestBuilder;
    private long limit;
    private Client client;
    //endregion


    //region Iterator
    private class ScrollIterator implements Iterator<SearchHit> {
        //region Constructor
        private ScrollIterator(SearchHitScrollIterable iterable) {
            this.iterable = iterable;
            iterable.getSearchRequestBuilder().setSearchType(SearchType.SCAN)
                    .setScroll(new TimeValue(getElasticGraphConfiguration().getElasticGraphScrollTime()))
                    .setSize(Math.min(getElasticGraphConfiguration().getElasticGraphScrollSize(),
                            (int)Math.min((long)Integer.MAX_VALUE, iterable.getLimit())));

            this.scrollId = null;
            this.searchHits = new ArrayList<SearchHit>(getElasticGraphConfiguration().getElasticGraphScrollSize());
        }
        //endregion

        //region Iterator Implementation
        @Override
        public boolean hasNext() {
            if (this.searchHits.size() > 0) {
                return true;
            }

            Scroll();

            if (this.searchHits.size() > 0) {
                return true;
            }

            return false;
        }

        @Override
        public SearchHit next() {
            if (this.searchHits.size() > 0) {
                SearchHit searchHit = this.searchHits.get(0);
                this.searchHits.remove(0);

                return searchHit;
            }

            Scroll();

            if (this.searchHits.size() > 0) {
                SearchHit searchHit = this.searchHits.get(0);
                this.searchHits.remove(0);

                return searchHit;
            }

            throw new NoSuchElementException();
        }
        //endregion

        //region Private Methods
        private void Scroll() {
            if (counter >= this.iterable.getLimit()) {
                return;
            }

            SearchResponse response;
            if (this.scrollId == null) {
                response = this.iterable.getSearchRequestBuilder()
                        .execute()
                        .actionGet();

                this.scrollId = response.getScrollId();
                Scroll();
            } else {
                response = this.iterable.getClient().prepareSearchScroll(this.scrollId)
                        .setScroll(new TimeValue(getElasticGraphConfiguration().getElasticGraphScrollTime()))
                        .execute()
                        .actionGet();

                for(SearchHit hit : response.getHits().getHits()) {
                    if (counter < this.iterable.getLimit()) {
                        this.searchHits.add(hit);
                        counter++;
                    }
                }

                this.scrollId = response.getScrollId();
            }
        }
        //endregion

        //region Fields
        private SearchHitScrollIterable iterable;
        private ArrayList<SearchHit> searchHits;
        private String scrollId;

        private long counter;
        //endregion
    }
    //endregion
}
