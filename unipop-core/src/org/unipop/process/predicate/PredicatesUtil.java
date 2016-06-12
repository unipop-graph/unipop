package org.unipop.process.predicate;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.util.HashSet;
import java.util.Set;

public class PredicatesUtil {

    public static void collectPredicates(ReceivesPredicatesHolder step, Traversal.Admin traversal){
        Step nextStep = step.getNextStep();
        Set<PredicatesHolder> predicates = new HashSet<>();


        while(true) {
            if(nextStep instanceof HasContainerHolder) {
                HasContainerHolder hasContainerHolder = (HasContainerHolder) nextStep;
                hasContainerHolder.getHasContainers().stream().map(PredicatesHolderFactory::predicate)
                        .forEach(predicates::add);
                traversal.removeStep(nextStep);
                if(collectLabels(nextStep, step)) break;
            }
//            else if (TraversalFilterStep.class.isAssignableFrom(nextStep.getClass())) {
//                TraversalFilterStep traversalFilterStep = (TraversalFilterStep)nextStep;
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
//                    collectLabels(predicates, nextStep);
//                    return predicates;
//                }
//            }
//            else if (PropertiesStep.class.isAssignableFrom(nextStep.getClass()) &&
//                    nextStep.equals(traversal.getEndStep()) &&
//                    TraversalFilterStep.class.isAssignableFrom(traversal.getParent().getClass())) {
//                PropertiesStep propertiesStep = (PropertiesStep)nextStep;
//                Arrays.asList(propertiesStep.getPropertyKeys()).forEach(propertyKey -> {
//                    predicates.hasContainers.add(new HasContainer(propertyKey, new ExistsP()));
//                });
//                traversal.removeStep(nextStep);
//
//                if(collectLabels(predicates, nextStep)) {
//                    return predicates;
//                }
//            }
            else if(nextStep instanceof RangeGlobalStep) {
                RangeGlobalStep rangeGlobalStep = (RangeGlobalStep) nextStep;
                int limit = rangeGlobalStep.getHighRange() > Integer.MAX_VALUE ? -1 : (int) rangeGlobalStep.getHighRange();
                step.setLimit(limit);
                collectLabels(nextStep, step);
                break;
            }
            else {

                break;
            }

            nextStep = nextStep.getNextStep();
        }

        PredicatesHolder predicate = PredicatesHolderFactory.and(predicates);
        step.addPredicate(predicate);
    }

    private static boolean collectLabels(Step<?, ?> step, Step<?, ?> originalStep) {
        step.getLabels().forEach(originalStep::addLabel);
        return step.getLabels().size() > 0;
    }
}
