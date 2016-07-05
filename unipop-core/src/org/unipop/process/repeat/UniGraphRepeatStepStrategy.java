package org.unipop.process.repeat;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ConnectiveStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.process.start.UniGraphStartStepStrategy;
import org.unipop.process.vertex.UniGraphVertexStepStrategy;
import org.unipop.structure.UniGraph;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by sbarzilay on 3/30/16.
 */
public class UniGraphRepeatStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        Set<Class<? extends TraversalStrategy.ProviderOptimizationStrategy>> priorStrategies = new HashSet<>();
        priorStrategies.add(UniGraphStartStepStrategy.class);
        priorStrategies.add(UniGraphVertexStepStrategy.class);
        return priorStrategies;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal)) return;

        Graph graph = traversal.getGraph().get();
        if (!(graph instanceof UniGraph)) {
            return;
        }

        UniGraph uniGraph = (UniGraph) graph;

        TraversalHelper.getStepsOfClass(RepeatStep.class, traversal).forEach(repeatStep -> {
            if (TraversalHelper.hasStepOfClass(UnionStep.class, (Traversal.Admin) repeatStep.getGlobalChildren().get(0))) {
                return;
            }
            UniGraphRepeatStep uniGraphRepeatStep = new UniGraphRepeatStep(repeatStep, traversal.asAdmin(), uniGraph);
            if (repeatStep.getUntilTraversal() != null && TraversalHelper.getFirstStepOfAssignableClass(ReducingBarrierStep.class, repeatStep.getUntilTraversal()).isPresent())
                return;
            Traversal.Admin<?, ?> repeatTraversal = uniGraphRepeatStep.getRepeatTraversal();
            TraversalHelper.replaceStep(repeatStep, uniGraphRepeatStep, traversal);
            TraversalHelper.getStepsOfClass(RepeatStep.RepeatEndStep.class, repeatTraversal).forEach(repeatEndStep -> {
                UniGraphRepeatStep.RepeatEndStep uniGraphRepeatEndStep = new UniGraphRepeatStep.RepeatEndStep(repeatTraversal, uniGraphRepeatStep);
                TraversalHelper.replaceStep(repeatEndStep, uniGraphRepeatEndStep, repeatTraversal);
            });
        });

    }
}
