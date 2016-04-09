package org.unipop.process.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.unipop.process.UniGraphRepeatStep;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by sbarzilay on 3/30/16.
 */
public class UniGraphRepeatStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        Set<Class<? extends TraversalStrategy.ProviderOptimizationStrategy>> priorStrategies = new HashSet<>();
        priorStrategies.add(UniGraphPredicatesStrategy.class);
        priorStrategies.add(UniGraphCountStepStrategy.class);
        priorStrategies.add(UniGraphStartStepStrategy.class);
        priorStrategies.add(UniGraphVertexStepStrategy.class);
        return priorStrategies;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfClass(RepeatStep.class, traversal).forEach(repeatStep -> {
            UniGraphRepeatStep uniGraphRepeatStep = new UniGraphRepeatStep(traversal, repeatStep);
            Traversal.Admin<?, ?> repeatTraversal = uniGraphRepeatStep.getRepeatTraversal();
            TraversalHelper.replaceStep(repeatStep, uniGraphRepeatStep, traversal);
            TraversalHelper.getStepsOfClass(RepeatStep.RepeatEndStep.class, repeatTraversal).forEach(repeatEndStep -> {
                UniGraphRepeatStep.UniGraphRepeatEndStep uniGraphRepeatEndStep = new UniGraphRepeatStep.UniGraphRepeatEndStep(traversal, uniGraphRepeatStep);
                TraversalHelper.replaceStep(repeatEndStep, uniGraphRepeatEndStep, repeatTraversal);
            });
        });

    }
}
