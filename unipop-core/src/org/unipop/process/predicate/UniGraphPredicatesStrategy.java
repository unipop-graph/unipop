package org.unipop.process.predicate;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.unipop.process.start.UniGraphStartStepStrategy;
import org.unipop.process.vertex.UniGraphVertexStepStrategy;

import java.util.*;

/**
 * Created by Roman on 11/8/2015.
 */
public class UniGraphPredicatesStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    //region AbstractTraversalStrategy Implementation
    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        Set<Class<? extends TraversalStrategy.ProviderOptimizationStrategy>> priorStrategies = new HashSet<>();
        priorStrategies.add(UniGraphStartStepStrategy.class);
        priorStrategies.add(UniGraphVertexStepStrategy.class);
        return priorStrategies;
    }


    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if(traversal.getEngine().isComputer()) {
            return;
        }

        TraversalHelper.getStepsOfAssignableClassRecursively(ReceivesHasContainers.class, traversal).forEach(step -> {
            addPredicates(step, traversal);
        });
    }
    //endregion

    //region Private Methods
    private void addPredicates(ReceivesHasContainers originalStep, Traversal.Admin traversal){
        Step step = originalStep;
        List<HasContainer> hasContainers = new ArrayList<>();

        while(true) {
            if(step instanceof HasContainerHolder) {
                HasContainerHolder hasContainerHolder = (HasContainerHolder) step;
                hasContainerHolder.getHasContainers().forEach(originalStep::addHasContainer);
                traversal.removeStep(step);
                if(collectLabels(step, originalStep)) return;
            }
//            else if (TraversalFilterStep.class.isAssignableFrom(step.getClass())) {
//                TraversalFilterStep traversalFilterStep = (TraversalFilterStep)step;
//                for(Object localChild : traversalFilterStep.getLocalChildren()) {
//                    Traversal.Admin filterTraversal = (Traversal.Admin)localChild;
//                    UniQuery childPredicates = addPredicates(filterTraversal.getStartStep(), filterTraversal);
//                    childPredicates.hasContainers.forEach(predicates.hasContainers::add);
//                    childPredicates.labels.forEach(predicates.labels::add);
//
//                    if (filterTraversal.getSteps().size() == 0) {
//                        traversal.removeStep(traversalFilterStep);
//                    }
//
//                    collectLabels(predicates, step);
//                    return predicates;
//                }
//            }
//            else if (PropertiesStep.class.isAssignableFrom(step.getClass()) &&
//                    step.equals(traversal.getEndStep()) &&
//                    TraversalFilterStep.class.isAssignableFrom(traversal.getParent().getClass())) {
//                PropertiesStep propertiesStep = (PropertiesStep)step;
//                Arrays.asList(propertiesStep.getPropertyKeys()).forEach(propertyKey -> {
//                    predicates.hasContainers.add(new HasContainer(propertyKey, new ExistsP()));
//                });
//                traversal.removeStep(step);
//
//                if(collectLabels(predicates, step)) {
//                    return predicates;
//                }
//            }
            else if(step instanceof RangeGlobalStep) {
                RangeGlobalStep rangeGlobalStep = (RangeGlobalStep) step;
                int limit = rangeGlobalStep.getHighRange() > Integer.MAX_VALUE ? 0 : (int) rangeGlobalStep.getHighRange();
                originalStep.setLimit(limit);
                collectLabels(step, originalStep);
                return;
            }
            else {
                return;
            }

            step = step.getNextStep();
        }
    }

    private boolean collectLabels(Step<?, ?> step, Step<?, ?> originalStep) {
        step.getLabels().forEach(originalStep::addLabel);
        return step.getLabels().size() > 0;
    }
    //endregion
}
