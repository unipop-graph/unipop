package org.unipop.query.aggregation.reduce;

import org.unipop.query.StepDescriptor;
import org.unipop.query.controller.UniQueryController;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.query.predicates.PredicatesHolder;

/**
 * @author Gur Ronen
 * @since 6/27/2016
 */
public class ReduceQuery extends PredicateQuery {
    public enum Op {
        COUNT,
        SUM,
        MEAN,
        MAX,
        MIN
    }

    private final Op op;
    private final String columnName;


    public ReduceQuery(PredicatesHolder predicates, StepDescriptor stepDescriptor, Op op, String columnName) {
        super(predicates, stepDescriptor);

        this.op = op;
        this.columnName = columnName;
    }

    public Op getOp() {
        return op;
    }

    public String getColumnName() {
        return columnName;
    }

    public interface ReduceController extends UniQueryController {
        Number reduce(ReduceQuery uniQuery);
    }
}
