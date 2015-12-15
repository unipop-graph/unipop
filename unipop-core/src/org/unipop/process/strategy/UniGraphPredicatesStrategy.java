package org.unipop.process.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.controller.ExistsP;
import org.unipop.controller.Predicates;
import org.unipop.process.UniGraphStartStep;
import org.unipop.process.UniGraphVertexStep;
import org.unipop.structure.UniGraph;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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

        Graph graph = traversal.getGraph().get();
        if(!(graph instanceof UniGraph)) {
            return;
        }

        UniGraph uniGraph = (UniGraph) graph;

        TraversalHelper.getStepsOfAssignableClassRecursively(UniGraphStartStep.class, traversal).forEach(elasticGraphStep -> {
            if(elasticGraphStep.getIds().length == 0) {
                Predicates predicates = getPredicates(elasticGraphStep.getNextStep(), traversal);
                elasticGraphStep.getPredicates().hasContainers.addAll(predicates.hasContainers);
                elasticGraphStep.getPredicates().labels.addAll(predicates.labels);
                elasticGraphStep.getPredicates().labels.forEach(label -> elasticGraphStep.addLabel(label));
                elasticGraphStep.getPredicates().limitHigh = predicates.limitHigh;
            }
        });

        TraversalHelper.getStepsOfAssignableClassRecursively(UniGraphVertexStep.class, traversal).forEach(elasticVertexStep -> {
            boolean returnVertex = elasticVertexStep.getReturnClass().equals(Vertex.class);
            Predicates predicates = returnVertex ? new Predicates() : getPredicates(elasticVertexStep.getNextStep(), traversal);
            elasticVertexStep.getPredicates().hasContainers.addAll(predicates.hasContainers);
            elasticVertexStep.getPredicates().labels.addAll(predicates.labels);
            elasticVertexStep.getPredicates().labels.forEach(label -> elasticVertexStep.addLabel(label));
            elasticVertexStep.getPredicates().limitHigh = predicates.limitHigh;
        });
    }
    //endregion

    //region Private Methods
    private Predicates getPredicates(Step step, Traversal.Admin traversal){
        Predicates predicates = new Predicates();

        while(true) {
            if(step instanceof HasContainerHolder) {
                HasContainerHolder hasContainerHolder = (HasContainerHolder) step;
                hasContainerHolder.getHasContainers().forEach(predicates.hasContainers::add);
                traversal.removeStep(step);

                if(collectLabels(predicates, step)) {
                    return predicates;
                }
            }
            else if (TraversalFilterStep.class.isAssignableFrom(step.getClass())) {
                TraversalFilterStep traversalFilterStep = (TraversalFilterStep)step;
                for(Object localChild : traversalFilterStep.getLocalChildren()) {
                    Traversal.Admin filterTraversal = (Traversal.Admin)localChild;
                    Predicates childPredicates = getPredicates(filterTraversal.getStartStep(), filterTraversal);
                    childPredicates.hasContainers.forEach(predicates.hasContainers::add);
                    childPredicates.labels.forEach(predicates.labels::add);

                    if (filterTraversal.getSteps().size() == 0) {
                        traversal.removeStep(traversalFilterStep);
                    }

                    collectLabels(predicates, step);
                    return predicates;
                }
            }
            else if (PropertiesStep.class.isAssignableFrom(step.getClass()) &&
                    step.equals(traversal.getEndStep()) &&
                    TraversalFilterStep.class.isAssignableFrom(traversal.getParent().getClass())) {
                PropertiesStep propertiesStep = (PropertiesStep)step;
                Arrays.asList(propertiesStep.getPropertyKeys()).forEach(propertyKey -> {
                    predicates.hasContainers.add(new HasContainer(propertyKey, new ExistsP()));
                });
                traversal.removeStep(step);

                if(collectLabels(predicates, step)) {
                    return predicates;
                }
            }
            else if(step instanceof RangeGlobalStep) {
                RangeGlobalStep rangeGlobalStep = (RangeGlobalStep) step;
                predicates.limitHigh = rangeGlobalStep.getHighRange();
                if(collectLabels(predicates, step)) return predicates;
            }
            else {
                return predicates;
            }

            step = step.getNextStep();
        }
    }

    private boolean collectLabels(Predicates predicates, Step<?, ?> step) {
        step.getLabels().forEach(predicates.labels::add);
        return step.getLabels().size() > 0;
    }
    //endregion
}
