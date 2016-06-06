package org.unipop.process.where;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.unipop.process.start.UniGraphStartStepStrategy;
import org.unipop.process.vertex.UniGraphVertexStepStrategy;
import org.unipop.process.where.UniGraphWhereTraversalStep;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by sbarzilay on 5/2/16.
 */
public class UniGraphWhereStepStartegy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        Set<Class<? extends TraversalStrategy.ProviderOptimizationStrategy>> priorStrategies = new HashSet<>();
        priorStrategies.add(UniGraphStartStepStrategy.class);
        priorStrategies.add(UniGraphVertexStepStrategy.class);
        return priorStrategies;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfClass(TraversalFilterStep.class, traversal).forEach(whereTraversalStep -> {
            if (!(((DefaultGraphTraversal) whereTraversalStep.getLocalChildren().get(0)).getStartStep() instanceof PropertiesStep)) {
                UniGraphWhereTraversalStep uniGraphWhereTraversalStep = new UniGraphWhereTraversalStep(traversal, (Traversal) whereTraversalStep.getLocalChildren().get(0));
                TraversalHelper.replaceStep(whereTraversalStep, uniGraphWhereTraversalStep, traversal);
            }
        });
        TraversalHelper.getStepsOfClass(WhereTraversalStep.class, traversal).forEach(whereTraversalStep -> {
            Traversal.Admin innerWhereTraversal = ((Traversal) whereTraversalStep.getLocalChildren().get(0)).asAdmin();
            TraversalHelper.getStepsOfClass(WhereTraversalStep.WhereStartStep.class, innerWhereTraversal).forEach(whereStartStep -> {
                UniGraphWhereTraversalStep.UniGraphWhereStartStep uniGraphWhereStartStep =
                        new UniGraphWhereTraversalStep.UniGraphWhereStartStep(innerWhereTraversal,
                                whereStartStep.getScopeKeys().iterator().next().toString());
                TraversalHelper.replaceStep(whereStartStep, uniGraphWhereStartStep, innerWhereTraversal);

            });
            TraversalHelper.getStepsOfClass(WhereTraversalStep.WhereEndStep.class, innerWhereTraversal).forEach(whereEndStep -> {
                UniGraphWhereTraversalStep.UniGraphWhereEndStep uniGraphWhereEndStep =
                        new UniGraphWhereTraversalStep.UniGraphWhereEndStep(innerWhereTraversal,
                                whereEndStep.getScopeKeys().iterator().next().toString());
                TraversalHelper.replaceStep(whereEndStep, uniGraphWhereEndStep, innerWhereTraversal);

            });
            UniGraphWhereTraversalStep uniGraphWhereTraversalStep = new UniGraphWhereTraversalStep(traversal, innerWhereTraversal);
            TraversalHelper.replaceStep(whereTraversalStep, uniGraphWhereTraversalStep, traversal);
        });
    }
}
