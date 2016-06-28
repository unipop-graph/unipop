package org.unipop.query.aggregation.reduce;

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanGlobalStep;
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
    public enum Op {
        COUNT(() -> 0L, (BinaryOperator) Operator.sumLong),
        SUM(() -> 0L, (BinaryOperator) Operator.sum),
        MEAN(() -> new MeanGlobalStep.MeanNumber(), new MeanGlobalStep.MeanGlobalBiOperator()),
        MAX(() -> Long.MIN_VALUE, (BinaryOperator) Operator.max),
        MIN(() -> Long.MAX_VALUE, (BinaryOperator) Operator.min);

            private final Supplier<Number> seedSupplier;
        private final BinaryOperator<Number> operator;

        Op(Supplier<Number> seedSupplier, BinaryOperator<Number> operator) {
            this.seedSupplier = seedSupplier;
            this.operator = operator;
        }

        public Supplier<Number> getSeedSupplier() {
            return this.seedSupplier;
        }

        public BinaryOperator<Number> getOperator() {
            return this.operator;
        }
    }

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

    public interface ReduceController extends UniQueryController {
        Number reduce(ReduceQuery uniQuery);
    }
}
