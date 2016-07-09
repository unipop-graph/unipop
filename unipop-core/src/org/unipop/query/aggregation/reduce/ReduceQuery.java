package org.unipop.query.aggregation.reduce;

import org.unipop.query.aggregation.reduce.controllers.ops.Op;
import org.unipop.query.StepDescriptor;
import org.unipop.query.controller.UniQueryController;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.query.predicates.PredicatesHolder;

import java.util.Set;

/**
 * @author Gur Ronen
 * @since 6/27/2016
 */
public class ReduceQuery extends PredicateQuery {

    private final Op op;
    private final Set<String> fieldNames;

    public ReduceQuery(PredicatesHolder predicates, StepDescriptor stepDescriptor, Op op, Set<String> fieldNames) {
        super(predicates, stepDescriptor);

        this.op = op;
        this.fieldNames = fieldNames;
    }

    public Op getOp() {
        return this.op;
    }

    public Set<String> getFieldNames() {
        return this.fieldNames;
    }

    //TODO: split to multiple interfaces per action, to avoid duplication in controllers
    public interface ReduceController extends UniQueryController {
        Number reduce(ReduceQuery uniQuery);
    }
}
