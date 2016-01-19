package org.unipop.elastic2.controller.schema.helpers.aggregationConverters;

import org.elasticsearch.search.aggregations.Aggregation;

/**
 * Created by Roman on 5/3/2015.
 */
public interface AggregationConverter<TAggregation extends Aggregation, TOutput> {
    boolean canConvert(TAggregation aggregation);
    TOutput convert(TAggregation aggregation);
}
