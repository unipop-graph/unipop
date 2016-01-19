package org.unipop.elastic2.controller.schema.helpers.aggregationConverters;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;

/**
 * Created by Roman on 5/27/2015.
 */
public class SingleValueAggregationConverter implements AggregationConverter<Aggregation, Double> {
    @Override
    public boolean canConvert(Aggregation aggregation) {
        return InternalNumericMetricsAggregation.SingleValue.class.isAssignableFrom(aggregation.getClass());
    }

    @Override
    public Double convert(Aggregation aggregation) {
        InternalNumericMetricsAggregation.SingleValue singleValue = (InternalNumericMetricsAggregation.SingleValue)aggregation;
        return singleValue.value();
    }
}
