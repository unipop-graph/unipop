package org.unipop.process.reduce.ops;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;

import java.util.function.BinaryOperator;
import java.util.function.Supplier;

/**
 * @author Gur Ronen
 * @since 6/29/2016
 */
public class Op {

    private final Supplier<Number> seedSupplier;
    private final BinaryOperator<Number> operator;
    private final Class<? extends ReducingBarrierStep> stepToReplace;

    public Op(Supplier<Number> seedSupplier, BinaryOperator<Number> operator, Class<? extends ReducingBarrierStep> stepToReplace) {
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
