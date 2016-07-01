package org.unipop.query.aggregation.reduce;

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.unipop.process.reduce.ops.Op;
import org.unipop.query.StepDescriptor;
import org.unipop.query.controller.UniQueryController;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.query.predicates.PredicatesHolder;

import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

/**
 * @author Gur Ronen
 * @since 6/27/2016
 */
public class ReduceQuery extends PredicateQuery {

    private final Op op;
    private final Set<String> fieldName;

    public ReduceQuery(PredicatesHolder predicates, StepDescriptor stepDescriptor, Op op, Set<String> fieldName) {
        super(predicates, stepDescriptor);

        this.op = op;
        this.fieldName = fieldName;
    }

    public Op getOp() {
        return this.op;
    }

    public Set<String> getFieldName() {
        return this.fieldName;
    }

    //TODO: split to multiple interfaces per action, to avoid duplication in controllers
    public interface ReduceController extends UniQueryController {
        Number reduce(ReduceQuery uniQuery);
    }
}
