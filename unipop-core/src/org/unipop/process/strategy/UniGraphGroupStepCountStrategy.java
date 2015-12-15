package org.unipop.process.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupCountStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.structure.UniGraph;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Roman on 11/12/2015.
 */
public class UniGraphGroupStepCountStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    //region AbstractTraversalStrategy Implementation
    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if (traversal.getEngine().isComputer()) return;

        Graph graph = traversal.getGraph().get();
        if (!(graph instanceof UniGraph)) return;

        TraversalHelper.getStepsOfAssignableClassRecursively(GroupCountStep.class, traversal).forEach(groupCountStep -> {
            Traversal keyTraversal = groupCountStep.getLocalChildren().size() > 0 ?
                    (Traversal)groupCountStep.getLocalChildren().get(0) :
                    __.map(traverser -> traverser.get());
            Traversal valueTraversal = keyTraversal.asAdmin().clone();
            Traversal reducerTraversal = __.count(Scope.local);

            GroupStep groupStep = new GroupStep(traversal);
            groupStep.addLocalChild(keyTraversal.asAdmin());
            groupStep.addLocalChild(valueTraversal.asAdmin());
            groupStep.addLocalChild(reducerTraversal.asAdmin());
            groupCountStep.getLabels().forEach(label -> groupStep.addLabel(label.toString()));

            TraversalHelper.replaceStep(groupCountStep, groupStep, traversal);
        });
    }
}
