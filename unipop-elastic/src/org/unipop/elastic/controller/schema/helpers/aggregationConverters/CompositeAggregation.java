package org.unipop.elastic.controller.schema.helpers.aggregationConverters;

import com.google.common.collect.FluentIterable;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.HasAggregations;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by Roman on 5/27/2015.
 */
public class CompositeAggregation implements Aggregation, HasAggregations {
    //region Constructor
    public CompositeAggregation(String name, Iterable<Aggregation> aggregations) {
        this.name = name;
        this.aggregations = new Aggregations(aggregations);
    }
    //endregion

    //region Aggregation implementation
    @Override
    public String getName() {
        return this.name;
    }
    //endregion

    //region HasAggregations Implementation
    @Override
    public Aggregations getAggregations() {
        return this.aggregations;
    }
    //endregion

    //region Fields
    private Aggregations aggregations;
    private String name;
    //endregion

    public class Aggregations implements org.elasticsearch.search.aggregations.Aggregations {
        //region Constructor
        public Aggregations(Iterable<Aggregation> aggregations) {
            this.aggregationMap = new HashMap<>();
            for(Aggregation aggregation : aggregations) {
                aggregationMap.put(aggregation.getName(), aggregation);
            }
        }
        //endregion

        //region Aggregations Implementation
        @Override
        public List<Aggregation> asList() {
            return FluentIterable.from(this.aggregationMap.values()).toList();
        }

        @Override
        public Map<String, Aggregation> asMap() {
            return this.aggregationMap;
        }

        @Override
        public Map<String, Aggregation> getAsMap() {
            return this.aggregationMap;
        }

        @Override
        public <A extends Aggregation> A get(String s) {
             return (A)this.aggregationMap.get(s);
        }

        @Override
        public Iterator<Aggregation> iterator() {
            return this.aggregationMap.values().iterator();
        }
        //endregion

        //region Fields
        Map<String, Aggregation> aggregationMap;
        //endregion
    }
}
