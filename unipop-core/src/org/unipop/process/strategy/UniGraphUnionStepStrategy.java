package org.unipop.process.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.unipop.process.UniGraphPropertiesSideEffectStep;
import org.unipop.process.UniGraphUnionStep;

import java.util.Set;

/**
 * Created by sbarzilay on 3/23/16.
 */
public class UniGraphUnionStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfClass(UnionStep.class, traversal).forEach(unionStep -> {
            Traversal.Admin[] traversals = (Traversal.Admin[]) unionStep.getGlobalChildren().toArray(new Traversal.Admin[0]);
            UniGraphUnionStep uniGraphUnionStep = new UniGraphUnionStep(traversal, traversals);
            TraversalHelper.replaceStep(unionStep, uniGraphUnionStep, traversal);
        });
    }
}
