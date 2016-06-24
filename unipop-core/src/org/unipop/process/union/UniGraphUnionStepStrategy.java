package org.unipop.process.union;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.structure.UniGraph;

/**
 * Created by sbarzilay on 3/23/16.
 */
public class UniGraphUnionStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        Graph graph = traversal.getGraph().get();
        if (!(graph instanceof UniGraph)) {
            return;
        }

        UniGraph uniGraph = (UniGraph) graph;

        TraversalHelper.getStepsOfClass(UnionStep.class, traversal).forEach(unionStep -> {
            Traversal.Admin[] traversals = (Traversal.Admin[]) unionStep.getGlobalChildren().toArray(new Traversal.Admin[0]);
            for (Traversal.Admin admin : traversals) {
                if (TraversalHelper.getLastStepOfAssignableClass(ReducingBarrierStep.class, admin).isPresent() ||
                        TraversalHelper.getLastStepOfAssignableClass(SelectOneStep.class, admin).isPresent() ||
                        TraversalHelper.getLastStepOfAssignableClass(SelectStep.class, admin).isPresent() ||
                        traversal.getParent() instanceof RepeatStep)
                    return;
            }
            UniGraphUnionStep uniGraphUnionStep = new UniGraphUnionStep(traversal, uniGraph, traversals);
            TraversalHelper.replaceStep(unionStep, uniGraphUnionStep, traversal);
        });
    }
}
