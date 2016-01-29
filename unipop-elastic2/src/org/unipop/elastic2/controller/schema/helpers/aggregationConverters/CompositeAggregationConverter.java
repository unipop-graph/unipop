package org.unipop.elastic2.controller.schema.helpers.aggregationConverters;

import org.elasticsearch.search.aggregations.Aggregation;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by Roman on 5/4/2015.
 */
public class CompositeAggregationConverter implements AggregationConverter<Aggregation, Object> {
    //region Constructor
    public CompositeAggregationConverter() {
        this.aggregationConverters = new ArrayList<>();
    }

    public CompositeAggregationConverter(Iterable<AggregationConverter<Aggregation, ?>> aggregationConverters) {
        this.aggregationConverters = aggregationConverters;
    }

    public CompositeAggregationConverter(AggregationConverter<Aggregation, ?>... aggregationConverters) {
        this.aggregationConverters = Arrays.asList(aggregationConverters);
    }
    //endregion

    //region AggregationConverter Implementation
    @Override
    public boolean canConvert(Aggregation aggregation) {
        for (AggregationConverter<Aggregation, ?> converter : this.aggregationConverters) {
            if (converter.canConvert(aggregation)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public Object convert(Aggregation aggregation) {
        for (AggregationConverter<Aggregation, ?> converter : this.aggregationConverters) {
            if (converter.canConvert(aggregation)) {
                return converter.convert(aggregation);
            }
        }

        return null;
    }
    //endregion

    //region Fields
    private Iterable<AggregationConverter<Aggregation, ?>> aggregationConverters;
    //endregion
}
