package org.unipop.query.aggregation.reduce.controllers.ops;

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BinaryOperator;

/**
 * @author Gur Ronen
 * @since 6/29/2016
 */
public class OpFactory {
    private List<Op> ops;

    public OpFactory() {
        this(Arrays.asList(
                new Op(() -> 0L, (BinaryOperator) Operator.sumLong, CountGlobalStep.class),
                new Op(() -> 0L, (BinaryOperator) Operator.sum, SumGlobalStep.class),
                new Op(MeanGlobalStep.MeanNumber::new, new MeanGlobalStep.MeanGlobalBiOperator(), MeanGlobalStep.class),
                new Op(() -> Long.MIN_VALUE, (BinaryOperator) Operator.max, MaxGlobalStep.class),
                new Op(() -> Long.MAX_VALUE, (BinaryOperator) Operator.min, MinGlobalStep.class)
        ));
    }

    public OpFactory(List<Op> ops) {
        this.ops = ops;
    }

    public Optional<Op> forClass(Class<? extends Step> clazz) {
        return ops.stream().filter(op -> op.getStepToReplace().equals(clazz)).findFirst();
    }

    public List<Op> getOps() { return this.ops; }
}
