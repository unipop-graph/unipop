package org.unipop.process.where;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.unipop.process.start.UniGraphStartStepStrategy;
import org.unipop.process.vertex.UniGraphVertexStepStrategy;
import org.unipop.process.where.UniGraphWhereTraversalStep;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by sbarzilay on 5/2/16.
 */
public class UniGraphWhereStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        Set<Class<? extends TraversalStrategy.ProviderOptimizationStrategy>> priorStrategies = new HashSet<>();
        priorStrategies.add(UniGraphStartStepStrategy.class);
        priorStrategies.add(UniGraphVertexStepStrategy.class);
        return priorStrategies;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.hasStepOfClass(MatchStep.class, traversal) || TraversalHelper.hasStepOfClass(MatchStep.MatchStartStep.class, traversal))
            return;
        TraversalHelper.getStepsOfClass(WhereTraversalStep.class, traversal).forEach(whereTraversalStep -> {
            Traversal.Admin innerWhereTraversal = ((Traversal) whereTraversalStep.getLocalChildren().get(0)).asAdmin();
            TraversalHelper.getStepsOfClass(WhereTraversalStep.WhereStartStep.class, innerWhereTraversal).forEach(whereStartStep -> {
                Iterator<String> iterator = whereStartStep.getScopeKeys().iterator();
                String selectKey = null;
                if (iterator.hasNext())
                    selectKey = iterator.next();
                UniGraphWhereTraversalStep.UniGraphWhereStartStep uniGraphWhereStartStep =
                        new UniGraphWhereTraversalStep.UniGraphWhereStartStep(innerWhereTraversal,
                                selectKey);
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
