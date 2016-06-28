package org.unipop.query.aggregation.reduce;

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
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
    //TODO: MUST MOVE TO INTERFACE / MODEL OBJECT INSTEAD OF ENUM
    public enum Op {
        COUNT(() -> 0L, (BinaryOperator) Operator.sumLong, CountGlobalStep.class),
        SUM(() -> 0L, (BinaryOperator) Operator.sum, SumGlobalStep.class),
        MEAN(MeanGlobalStep.MeanNumber::new, new MeanGlobalStep.MeanGlobalBiOperator(), MeanGlobalStep.class),
        MAX(() -> Long.MIN_VALUE, (BinaryOperator) Operator.max, MaxGlobalStep.class),
        MIN(() -> Long.MAX_VALUE, (BinaryOperator) Operator.min, MinGlobalStep.class);

        private final Supplier<Number> seedSupplier;
        private final BinaryOperator<Number> operator;
        private final Class<? extends ReducingBarrierStep> stepToReplace;

        Op(Supplier<Number> seedSupplier, BinaryOperator<Number> operator, Class<? extends ReducingBarrierStep> stepToReplace) {
            this.seedSupplier = seedSupplier;
            this.operator = operator;
            this.stepToReplace = stepToReplace;
        }

        public Supplier<Number> getSeedSupplier() {
            return this.seedSupplier;
        }

        public BinaryOperator<Number> getOperator() {
            return this.operator;
        }

        public Class<? extends ReducingBarrierStep> getStepToReplace() {
            return this.stepToReplace;
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

    //TODO: split to multiple interfaces per action, to avoid duplication in controllers
    public interface ReduceController extends UniQueryController {
        Number reduce(ReduceQuery uniQuery);
    }
}
