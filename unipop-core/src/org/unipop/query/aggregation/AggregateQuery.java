package org.unipop.query.aggregation;

import org.unipop.process.group.traversal.SemanticKeyTraversal;
import org.unipop.process.group.traversal.SemanticReducerTraversal;
import org.unipop.process.group.traversal.SemanticValuesTraversal;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.StepDescriptor;
import org.unipop.query.controller.UniQueryController;
import org.unipop.query.predicates.PredicateQuery;

import java.util.Map;

public class AggregateQuery extends PredicateQuery {
    private final SemanticKeyTraversal key;
    private final SemanticValuesTraversal values;
    private final SemanticReducerTraversal reduce;

    public AggregateQuery(PredicatesHolder predicates,
                          SemanticKeyTraversal key,
                          SemanticValuesTraversal values,
                          SemanticReducerTraversal reduce,
                          StepDescriptor stepDescriptor) {
        super(predicates, stepDescriptor);
        this.key = key;
        this.values = values;
        this.reduce = reduce;
    }

    public SemanticKeyTraversal getKey() {
        return key;
    }

    public SemanticValuesTraversal getValues() {
        return values;
    }

    public SemanticReducerTraversal getReduce() {
        return reduce;
    }

    public interface AggregationController extends UniQueryController {
        Map<String, Object> query(AggregateQuery uniQuery);
    }

    @Override
    public String toString() {
        return "AggregateQuery{" +
                "key=" + key +
                ", values=" + values +
                ", reduce=" + reduce +
                '}';
    }
}
