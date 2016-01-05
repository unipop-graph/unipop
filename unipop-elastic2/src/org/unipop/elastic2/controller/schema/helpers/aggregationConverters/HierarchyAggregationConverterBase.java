package org.unipop.elastic2.controller.schema.helpers.aggregationConverters;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.HasAggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

/**
 * Created by Roman on 5/3/2015.
 */
public abstract class HierarchyAggregationConverterBase<TAggregation extends Aggregation, TOutput> implements
    AggregationConverter<TAggregation, TOutput> {

    //region Constructor
    public HierarchyAggregationConverterBase() {
    }

    public HierarchyAggregationConverterBase(AggregationConverter<Aggregation, Object> innerConverter) {
        this.innerConverter = innerConverter;
    }
    //endregion

    //region AggregationConverter Implementation
    @Override
    public boolean canConvert(TAggregation aggregation) {
        return HasAggregations.class.isAssignableFrom(aggregation.getClass()) ||
                MultiBucketsAggregation.class.isAssignableFrom(aggregation.getClass());
    }

    @Override
    public TOutput convert(TAggregation aggregation) {
        TOutput output = initializeOutput();

        if (MultiBucketsAggregation.class.isAssignableFrom(aggregation.getClass())) {
            MultiBucketsAggregation multiBucketAggregation = (MultiBucketsAggregation)aggregation;

            for(MultiBucketsAggregation.Bucket bucket : multiBucketAggregation.getBuckets()) {
                output = mergeBucket(output, bucket);
                for (Aggregation childAggregation : bucket.getAggregations()) {
                    if (this.innerConverter != null) {
                        if (this.innerConverter.canConvert(childAggregation)) {
                            Object childOutput = this.innerConverter.convert(childAggregation);
                            output = mergeChildOutput(output, bucket, childAggregation, childOutput);
                        }
                    }
                }
            }
        } else if (HasAggregations.class.isAssignableFrom(aggregation.getClass())) {
            for (Aggregation childAggregation : ((HasAggregations) aggregation).getAggregations()) {
                if (this.innerConverter != null) {
                    if (this.innerConverter.canConvert(childAggregation)) {
                        Object childOutput = this.innerConverter.convert(childAggregation);
                        output = mergeChildOutput(output, (HasAggregations) aggregation, childAggregation, childOutput);
                    }
                }
            }
        }

        return output;
    }
    //endregion

    //region Abstract Methods
    protected abstract TOutput initializeOutput();
    protected abstract TOutput mergeBucket(TOutput output, MultiBucketsAggregation.Bucket bucket);
    protected abstract TOutput mergeChildOutput(TOutput output, HasAggregations aggregationParent , Aggregation childAggregation, Object childOutput);
    //endregion

    //region properties
    public AggregationConverter getInnerConverter() {
        return this.innerConverter;
    }
    public void setInnerConverter(AggregationConverter<Aggregation, Object> value) {
        this.innerConverter = value;
    }
    //endregion

    //region Fields
    private AggregationConverter<Aggregation, Object> innerConverter;
    //endregion
}
