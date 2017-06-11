package org.unipop.process.local;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SampleLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.unipop.process.UniQueryStep;
import org.unipop.process.edge.EdgeStepsStrategy;
import org.unipop.process.properties.UniGraphPropertiesStrategy;
import org.unipop.process.reduce.UniGraphReduceStrategy;
import org.unipop.process.start.UniGraphStartStepStrategy;
import org.unipop.process.vertex.UniGraphVertexStep;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.structure.UniGraph;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 9/4/16.
 */
public class UniGraphLocalStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        return Sets.newHashSet(UniGraphStartStepStrategy.class, UniGraphPropertiesStrategy.class, UniGraphReduceStrategy.class, EdgeStepsStrategy.class);
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        UniGraph uniGraph = (UniGraph) traversal.getGraph().get();
        List<LocalQuery.LocalController> localControllers = uniGraph.getControllerManager()
                .getControllers(LocalQuery.LocalController.class);

        List<SearchVertexQuery.SearchVertexController> nonLocalControllers = uniGraph.getControllerManager().getControllers(SearchVertexQuery.SearchVertexController.class)
                .stream().filter(controller -> !localControllers.contains(controller)).collect(Collectors.toList());

        TraversalHelper.getStepsOfAssignableClass(LocalStep.class, traversal).forEach(localStep -> {
            Traversal.Admin localTraversal = (Traversal.Admin) localStep.getLocalChildren().get(0);
            if (TraversalHelper.hasStepOfAssignableClass(SampleGlobalStep.class, localTraversal) ||
                    TraversalHelper.hasStepOfAssignableClass(SampleLocalStep.class, localTraversal)){
                return;
            }
            if (TraversalHelper.hasStepOfAssignableClass(UniQueryStep.class, localTraversal)) {
                localTraversal.getSteps().stream().filter(step -> step instanceof UniGraphVertexStep).forEach(step -> ((UniGraphVertexStep) step).setControllers(nonLocalControllers));
                UniGraphLocalStep uniGraphLocalStep = new UniGraphLocalStep(traversal, localTraversal, localControllers);
                TraversalHelper.replaceStep(localStep, uniGraphLocalStep, traversal);
            }
        });

        TraversalHelper.getStepsOfAssignableClass(ProjectStep.class, traversal).forEach(projectStep -> {
            UniGraphProjectStep uniGraphProjectStep = new UniGraphProjectStep(traversal, uniGraph,
                    (String[]) projectStep.getProjectKeys().toArray(new String[projectStep.getProjectKeys().size()]),
                    localControllers, nonLocalControllers, projectStep.getLocalChildren());
            projectStep.getLabels().forEach(label -> uniGraphProjectStep.addLabel(label.toString()));
            TraversalHelper.replaceStep(projectStep, uniGraphProjectStep, traversal);
        });

//        TraversalHelper.getStepsOfAssignableClass(GroupStep.class, traversal).forEach(groupStep -> {
//            Traversal.Admin localTraversal = (Traversal.Admin) groupStep.getLocalChildren().get(1);
//            if (TraversalHelper.hasStepOfAssignableClass(SampleGlobalStep.class, localTraversal) ||
//                    TraversalHelper.hasStepOfAssignableClass(SampleLocalStep.class, localTraversal)){
//                return;
//            }
//            UniGraphGroupStep uniGraphGroupStep = new UniGraphGroupStep(traversal, uniGraph, localControllers, nonLocalControllers, groupStep);
//            TraversalHelper.replaceStep(groupStep, uniGraphGroupStep, traversal);
//        });
    }
}
