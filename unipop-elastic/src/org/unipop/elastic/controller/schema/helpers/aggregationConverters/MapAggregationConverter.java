package org.unipop.elastic.controller.schema.helpers.aggregationConverters;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.Collection;
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
                stringObjectMap.put(bucket.getKey(), bucket.getDocCount());
            } else {
                Map<String, Object> bucketMap = null;
                Object bucketObj = stringObjectMap.get(bucket.getKey());
                if (bucketObj == null) {
                    bucketMap = new HashMap<>();
                    bucketMap.put("count", bucket.getDocCount());
                    stringObjectMap.put(bucket.getKey(), bucketMap);
                } else {
                    bucketMap = (HashMap<String, Object>) bucketObj;
                }
            }
        }

        return stringObjectMap;
    }

    @Override
    protected Map<String, Object> mergeBuckets(Map<String, Object> stringObjectMap, Collection<? extends MultiBucketsAggregation.Bucket> buckets) {
        return stringObjectMap;
    }

    @Override
    protected Map<String, Object> mergeChildOutput(Map<String, Object> stringObjectMap, MultiBucketsAggregation.Bucket bucket, Aggregation childAggregation, Object childOutput) {
        if (bucket != null) {
            mergeBucket(stringObjectMap, bucket);
            if (getUseSimpleFormat()) {
                stringObjectMap.put(bucket.getKey(), childOutput);
            } else {
                Map<String, Object> bucketMap = (Map<String, Object>) stringObjectMap.get(bucket.getKey());
                bucketMap.put(childAggregation.getName(), childOutput);
            }
        } else {
            /*if (getUseSimpleFormat()) {
                if (Map.class.isAssignableFrom(childOutput.getClass())) {
                    ((Map<String, Object>)childOutput).entrySet().forEach(entry -> {
                        stringObjectMap.put(entry.getKey(), entry.getValue());
                    });
                }
            } else {*/
                stringObjectMap.put(childAggregation.getName(), childOutput);
            //}
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
