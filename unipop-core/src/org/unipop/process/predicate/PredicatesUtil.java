package org.unipop.process.predicate;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PredicatesUtil {

    public static void collectPredicates(ReceivesPredicatesHolder step, Traversal.Admin traversal){
        Step nextStep = step.getNextStep();
        Set<PredicatesHolder> predicates = new HashSet<>();


        while(true) {
            if(nextStep instanceof HasContainerHolder) {
                HasContainerHolder<?, ?> hasContainerHolder = (HasContainerHolder<?, ?>) nextStep;
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
                int limit = rangeGlobalStep.getHighRange() > Integer.MAX_VALUE ? -1 : rangeGlobalStep.getHighRange().intValue();
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

    /**
     * Fold the has()-steps that immediately follow a vertex-producing step into a predicates holder,
     * removing them from the traversal, so they can be pushed into the produced-vertex fetch. Unlike
     * {@link #collectPredicates}, the result is returned (not added to the step as edge predicates),
     * because these filter the produced VERTEX, not the adjacency edge query. Stops at the first
     * non-HasContainerHolder step (transparently skipping NoOpBarrierStep inserted by LazyBarrierStrategy).
     */
    public static PredicatesHolder collectVertexPredicates(Step<?, ?> step, Traversal.Admin traversal) {
        Set<PredicatesHolder> predicates = new HashSet<>();
        // Collect any NoOpBarrierStep instances to remove once we know there are has() steps to fold.
        List<Step<?, ?>> barrierStepsToRemove = new ArrayList<>();
        Step<?, ?> nextStep = step.getNextStep();
        // Skip transparent NoOpBarrierStep(s) inserted by LazyBarrierStrategy before looking for has() steps.
        while (nextStep instanceof NoOpBarrierStep) {
            barrierStepsToRemove.add(nextStep);
            nextStep = nextStep.getNextStep();
        }
        while (nextStep instanceof HasContainerHolder) {
            HasContainerHolder<?, ?> hasContainerHolder = (HasContainerHolder<?, ?>) nextStep;
            hasContainerHolder.getHasContainers().stream().map(PredicatesHolderFactory::predicate).forEach(predicates::add);
            Step<?, ?> toRemove = nextStep;
            boolean hasLabels = !nextStep.getLabels().isEmpty();
            nextStep.getLabels().forEach(step::addLabel);
            nextStep = nextStep.getNextStep();
            traversal.removeStep(toRemove);
            if (hasLabels) break;
        }
        // If we found and consumed has() steps, remove any barrier steps we skipped over — they are
        // now either between the vertex step and non-has steps (identity passthrough, not needed) or
        // are skipped over correctly. Removing them avoids double-buffering.
        if (!predicates.isEmpty()) {
            barrierStepsToRemove.forEach(traversal::removeStep);
        }
        return PredicatesHolderFactory.and(predicates);
    }
}
