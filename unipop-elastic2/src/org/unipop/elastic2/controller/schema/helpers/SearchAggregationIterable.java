package org.unipop.elastic2.controller.schema.helpers;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.aggregations.Aggregation;
import org.unipop.structure.UniGraph;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by Gilad on 04/11/2015.
 */
public class SearchAggregationIterable implements Iterable<Aggregation> {
    //region Constructor
    public SearchAggregationIterable(UniGraph graph,
                                     SearchRequestBuilder searchRequestBuilder,
                                     Client client) {
        this.graph = graph;
        this.searchRequestBuilder = searchRequestBuilder;
        this.client = client;
    }
    //endregion

    //region Iterable Implementation
    @Override
    public Iterator<Aggregation> iterator() {
        return new BucketIterator(this);
    }
    //endregion

    //region Properties
    protected UniGraph getGraph() {
        return this.graph;
    }

    protected SearchRequestBuilder getSearchRequestBuilder() {
        return this.searchRequestBuilder;
    }

    protected Client getClient() {
        return this.client;
    }
    //endregion

    //region Fields
    private UniGraph graph;
    private SearchRequestBuilder searchRequestBuilder;
    private Client client;
    //endregion

    public class BucketIterator implements Iterator<Aggregation> {
        //region Constructor
        public BucketIterator(SearchAggregationIterable iterable) {
            this.iterable = iterable;
            iterable.getSearchRequestBuilder().setSearchType(SearchType.COUNT);

            this.aggregations = new ArrayList<>();
        }
        //endregion

        //region Iterator Implementation
        @Override
        public boolean hasNext() {
            if (this.aggregations.size() > 0) {
                return true;
            }

            fetchAggregations();

            if (this.aggregations.size() > 0) {
                return true;
            }

            return false;
        }

        @Override
        public Aggregation next() {
            if (this.aggregations.size() > 0) {
                Aggregation aggregation = this.aggregations.get(0);
                this.aggregations.remove(0);

                return aggregation;
            }

            fetchAggregations();

            if (this.aggregations.size() > 0) {
                Aggregation aggregation = this.aggregations.get(0);
                this.aggregations.remove(0);

                return aggregation;
            }

            throw new NoSuchElementException();
        }
        //endregion

        //region Private Methods
        private void fetchAggregations() {
            if (!aggregationsFetched) {
                SearchResponse response = this.iterable.getSearchRequestBuilder()
                        .execute()
                        .actionGet();

                for (Aggregation aggregation : response.getAggregations()) {
                    this.aggregations.add(aggregation);
                }

                aggregationsFetched = true;
            }
        }
        //endregion

        //region Fields
        private SearchAggregationIterable iterable;
        private ArrayList<Aggregation> aggregations;
        private boolean aggregationsFetched = false;
        //endregion
    }
}
