package org.unipop.elastic2.controller.schema.helpers.aggregationConverters;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Roman on 5/31/2015.
 */
public class StatsAggregationConverter implements AggregationConverter<Aggregation, Map<String, Object>> {
    //region AggregationConverter Implementation
    @Override
    public boolean canConvert(Aggregation aggregation) {
        return Stats.class.isAssignableFrom(aggregation.getClass());
    }

    @Override
    public Map<String, Object> convert(Aggregation aggregation) {
        Stats stats = (Stats)aggregation;

        Map<String, Object> map = new HashMap<>();
        map.put("count", stats.getCount());
        map.put("min", stats.getMin());
        map.put("avg", stats.getAvg());
        map.put("max", stats.getMax());
        map.put("sum", stats.getSum());

        return map;
    }
    //endregion
}
