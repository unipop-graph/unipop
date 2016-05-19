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
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.util.*;

public class UniGraphPredicatesStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if(traversal.getEngine().isComputer()) {
            return;
        }

        TraversalHelper.getStepsOfAssignableClassRecursively(ReceivesPredicatesHolder.class, traversal).forEach(step ->
                addPredicates(step, traversal));
    }

    private void addPredicates(ReceivesPredicatesHolder originalStep, Traversal.Admin traversal){
        Step step = originalStep.getNextStep();
        Set<PredicatesHolder> predicates = new HashSet<>();


        while(true) {
            if(step instanceof HasContainerHolder) {
                HasContainerHolder hasContainerHolder = (HasContainerHolder) step;
                hasContainerHolder.getHasContainers().stream().map(PredicatesHolderFactory::predicate)
                        .forEach(predicates::add);
                traversal.removeStep(step);
                if(collectLabels(step, originalStep)) break;
            }
//            else if (TraversalFilterStep.class.isAssignableFrom(step.getClass())) {
//                TraversalFilterStep traversalFilterStep = (TraversalFilterStep)step;
//                for(Object localChild : traversalFilterStep.getLocalChildren()) {
//                    Traversal.Admin filterTraversal = (Traversal.Admin)localChild;
//                    UniQuery childPredicates = addPredicate(filterTraversal.getStartStep(), filterTraversal);
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
                int limit = rangeGlobalStep.getHighRange() > Integer.MAX_VALUE ? -1 : (int) rangeGlobalStep.getHighRange();
                originalStep.setLimit(limit);
                collectLabels(step, originalStep);
                break;
            }
            else {

                break;
            }

            step = step.getNextStep();
        }

        PredicatesHolder predicate = PredicatesHolderFactory.and(predicates);
        originalStep.addPredicate(predicate);
    }

    private boolean collectLabels(Step<?, ?> step, Step<?, ?> originalStep) {
        step.getLabels().forEach(originalStep::addLabel);
        return step.getLabels().size() > 0;
    }
}