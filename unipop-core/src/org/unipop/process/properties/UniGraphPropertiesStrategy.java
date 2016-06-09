package org.unipop.process.properties;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.unipop.process.edge.UniGraphEdgeOtherVertexStep;
import org.unipop.process.start.UniGraphStartStep;
import org.unipop.process.start.UniGraphStartStepStrategy;
import org.unipop.process.vertex.UniGraphVertexStep;
import org.unipop.process.vertex.UniGraphVertexStepStrategy;
import org.unipop.structure.UniGraph;

import java.util.List;
import java.util.Set;

/**
 * Created by sbarzilay on 6/8/16.
 */
public class UniGraphPropertiesStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        return Sets.newHashSet(UniGraphStartStepStrategy.class, UniGraphVertexStepStrategy.class);
    }

    private void handlePropertiesSteps(String[] propertyKeys, PropertyFetcher propertyFetcher) {
        if (propertyFetcher != null) {
            if (propertyKeys.length > 0)
                for (String key : propertyKeys) {
                    propertyFetcher.addPropertyKey(key);
                }
            else
                propertyFetcher.fetchAllKeys();
        }
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        // TODO: minimize the use of UniGraphVertexPropertiesSideEffectStep
        UniGraph uniGraph = (UniGraph) traversal.getGraph().get();

        TraversalHelper.getStepsOfClass(PropertyMapStep.class, traversal).forEach(propertyMapStep -> {
            PropertyFetcher propertyFetcher = getPropertyFetcherStepOf(propertyMapStep, traversal);
            if (propertyFetcher == null || (traversal.getParent() instanceof ConnectiveStep) || TraversalHelper.hasStepOfClass(MatchStep.MatchStartStep.class, traversal)) {
                UniGraphVertexPropertiesSideEffectStep uniGraphVertexPropertiesSideEffectStep = new UniGraphVertexPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
                TraversalHelper.insertBeforeStep(uniGraphVertexPropertiesSideEffectStep, propertyMapStep, traversal);
                return;
            }
            handlePropertiesSteps(propertyMapStep.getPropertyKeys(), propertyFetcher);
        });

        TraversalHelper.getStepsOfClass(PropertiesStep.class, traversal).forEach(propertiesStep -> {
            PropertyFetcher propertyFetcher = getPropertyFetcherStepOf(propertiesStep, traversal);
            if (propertyFetcher == null || (traversal.getParent() instanceof ConnectiveStep) || TraversalHelper.hasStepOfClass(MatchStep.MatchStartStep.class, traversal)) {
                UniGraphVertexPropertiesSideEffectStep uniGraphVertexPropertiesSideEffectStep = new UniGraphVertexPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
                TraversalHelper.insertBeforeStep(uniGraphVertexPropertiesSideEffectStep, propertiesStep, traversal);
                return;
            }
            handlePropertiesSteps(propertiesStep.getPropertyKeys(), propertyFetcher);
        });

        TraversalHelper.getStepsOfClass(HasStep.class, traversal).forEach(hasStep -> {
            PropertyFetcher propertyFetcher = getPropertyFetcherStepOf(hasStep, traversal);
            if (propertyFetcher == null || (traversal.getParent() instanceof ConnectiveStep) || TraversalHelper.hasStepOfClass(MatchStep.MatchStartStep.class, traversal)) {
                UniGraphVertexPropertiesSideEffectStep uniGraphVertexPropertiesSideEffectStep = new UniGraphVertexPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
                TraversalHelper.insertBeforeStep(uniGraphVertexPropertiesSideEffectStep, hasStep, traversal);
                return;
            }
            List<HasContainer> hasContainers = hasStep.getHasContainers();
            hasContainers.stream().map(HasContainer::getKey).forEach(propertyFetcher::addPropertyKey);
        });

        TraversalHelper.getStepsOfClass(WherePredicateStep.class, traversal).forEach(wherePredicateStep -> {
            PropertyFetcher propertyFetcher = getPropertyFetcherStepOf(wherePredicateStep, traversal);
            if (propertyFetcher == null || (traversal.getParent() instanceof ConnectiveStep) || TraversalHelper.hasStepOfClass(MatchStep.MatchStartStep.class, traversal)) {
                UniGraphVertexPropertiesSideEffectStep uniGraphVertexPropertiesSideEffectStep = new UniGraphVertexPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
                TraversalHelper.insertBeforeStep(uniGraphVertexPropertiesSideEffectStep, wherePredicateStep, traversal);
                return;
            }
            propertyFetcher.fetchAllKeys();
        });

        TraversalHelper.getStepsOfAssignableClass(FilterStep.class, traversal).forEach(filterStep -> {
            if (!(filterStep instanceof HasStep) && !(filterStep instanceof WherePredicateStep)) {
                UniGraphVertexPropertiesSideEffectStep uniGraphVertexPropertiesSideEffectStep = new UniGraphVertexPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
                TraversalHelper.insertBeforeStep(uniGraphVertexPropertiesSideEffectStep, filterStep, traversal);
            }
        });

        TraversalHelper.getStepsOfAssignableClass(MapStep.class, traversal).forEach(mapStep -> {
            if (!(mapStep instanceof PropertyMapStep)) {
                UniGraphVertexPropertiesSideEffectStep uniGraphVertexPropertiesSideEffectStep = new UniGraphVertexPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
                TraversalHelper.insertBeforeStep(uniGraphVertexPropertiesSideEffectStep, mapStep, traversal);
            }
        });

        TraversalHelper.getStepsOfAssignableClass(ReducingBarrierStep.class, traversal).forEach(reducingBarrierStep -> {
            UniGraphVertexPropertiesSideEffectStep uniGraphVertexPropertiesSideEffectStep = new UniGraphVertexPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
            TraversalHelper.insertBeforeStep(uniGraphVertexPropertiesSideEffectStep, reducingBarrierStep, traversal);
        });

        TraversalHelper.getStepsOfAssignableClass(SideEffectStep.class, traversal).forEach(sideEffectStep -> {
            UniGraphVertexPropertiesSideEffectStep uniGraphVertexPropertiesSideEffectStep = new UniGraphVertexPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
            TraversalHelper.insertBeforeStep(uniGraphVertexPropertiesSideEffectStep, sideEffectStep, traversal);
        });
//        traversal.addStep(new UniGraphVertexPropertiesSideEffectStep(traversal, uniGraph.getControllerManager()));
    }


    private PropertyFetcher getPropertyFetcherStepOf(Step step, Traversal.Admin<?, ?> traversal) {
        Step previous = step.getPreviousStep();
        while (!(previous instanceof PropertyFetcher)) {
            if (previous instanceof UniGraphStartStep || previous instanceof WhereTraversalStep.WhereStartStep || previous instanceof TraversalParent)
                return null;
            if (previous instanceof EmptyStep) {
                previous = traversal.getParent().asStep();
            }
            previous = previous.getPreviousStep();
        }
        return (PropertyFetcher) previous;
    }
}
