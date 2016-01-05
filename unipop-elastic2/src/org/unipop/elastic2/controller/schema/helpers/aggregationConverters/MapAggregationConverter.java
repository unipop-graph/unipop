package org.unipop.elastic2.controller.schema.helpers.aggregationConverters;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.HasAggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Roman on 5/3/2015.
 */
public class MapAggregationConverter extends HierarchyAggregationConverterBase<Aggregation, Map<String, Object>> {
    //region Constructor
    public MapAggregationConverter() {
    }

    public MapAggregationConverter(AggregationConverter<Aggregation, Object> innerConverter) {
        super(innerConverter);
    }
    //endregion

    //region AggregationConverterBase Implementation
    @Override
    protected Map<String, Object> initializeOutput() {
        return new HashMap<>();
    }

    @Override
    protected Map<String, Object> mergeBucket(Map<String, Object> stringObjectMap, MultiBucketsAggregation.Bucket bucket) {
        if (bucket != null) {
            if (getUseSimpleFormat()) {
                stringObjectMap.put((String) bucket.getKey(), bucket.getDocCount());
            } else {
                Map<String, Object> bucketMap = null;
                Object bucketObj = stringObjectMap.get(bucket.getKey());
                if (bucketObj == null) {
                    bucketMap = new HashMap<>();
                    bucketMap.put("count", bucket.getDocCount());
                    stringObjectMap.put((String) bucket.getKey(), bucketMap);
                }
            }
        }

        return stringObjectMap;
    }

    @Override
    protected Map<String, Object> mergeChildOutput(Map<String, Object> stringObjectMap, HasAggregations aggregationParent, Aggregation childAggregation, Object childOutput) {
        MultiBucketsAggregation.Bucket bucket = MultiBucketsAggregation.Bucket.class.isAssignableFrom(aggregationParent.getClass()) ?
                (MultiBucketsAggregation.Bucket)aggregationParent : null;

        if (bucket != null) {
            mergeBucket(stringObjectMap, bucket);
            if (getUseSimpleFormat() && aggregationParent.getAggregations().asList().size() == 1) {
                stringObjectMap.put((String) bucket.getKey(), childOutput);
            } else {
                Map<String, Object> bucketMap = (Map<String, Object>) stringObjectMap.get(bucket.getKey());
                bucketMap.put(childAggregation.getName(), childOutput);
            }
        } else {
            if (getUseSimpleFormat() && aggregationParent.getAggregations().asList().size() == 1) {
                if (Map.class.isAssignableFrom(childOutput.getClass())) {
                    stringObjectMap = (Map<String, Object>)childOutput;
                }
            } else {
                stringObjectMap.put(childAggregation.getName(), childOutput);
            }
        }

        return stringObjectMap;
    }
    //endregion

    //region Properties
    public boolean getUseSimpleFormat() {
        return this.useSimpleFormat;
    }

    public void setUseSimpleFormat(boolean value) {
        this.useSimpleFormat = value;
    }
    //endregion

    //region Fields
    private boolean useSimpleFormat;
    //endregion
}
