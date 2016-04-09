package org.unipop.process.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.process.UniGraphCountStep;
import org.unipop.process.UniGraphPropertiesSideEffectStep;
import org.unipop.process.UniGraphVertexStepWithProperties;
import org.unipop.structure.UniGraph;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;


/**
 * Created by sbarzilay on 3/9/16.
 */
public class UniGraphPropertiesStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        Set<Class<? extends TraversalStrategy.ProviderOptimizationStrategy>> priorStrategies = new HashSet<>();
        priorStrategies.add(UniGraphPredicatesStrategy.class);
        priorStrategies.add(UniGraphCountStepStrategy.class);
        priorStrategies.add(UniGraphStartStepStrategy.class);
        priorStrategies.add(UniGraphVertexStepStrategy.class);
        return priorStrategies;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if (traversal.getEngine().isComputer()) {
            return;
        }

        Graph graph = traversal.getGraph().get();
        if (!(graph instanceof UniGraph)) {
            return;
        }

        UniGraph uniGraph = (UniGraph) graph;


        TraversalHelper.getStepsOfClass(PropertiesStep.class, traversal).forEach(propertiesStep -> {
            if (!(propertiesStep.getPreviousStep() instanceof UniGraphVertexStepWithProperties)) {
                UniGraphPropertiesSideEffectStep uniGraphPropertiesSideEffectStep = new UniGraphPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
                TraversalHelper.insertBeforeStep(uniGraphPropertiesSideEffectStep, propertiesStep, traversal);
            }
        });

        TraversalHelper.getStepsOfClass(HasStep.class, traversal).forEach(hasStep -> {
            if (!(hasStep.getPreviousStep() instanceof UniGraphVertexStepWithProperties)) {
                UniGraphPropertiesSideEffectStep uniGraphPropertiesSideEffectStep = new UniGraphPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
                TraversalHelper.insertBeforeStep(uniGraphPropertiesSideEffectStep, hasStep, traversal);
            }
        });

        TraversalHelper.getStepsOfAssignableClass(FilterStep.class, traversal).forEach(filterStep -> {
            if (!(filterStep.getPreviousStep() instanceof UniGraphVertexStepWithProperties)) {
                UniGraphPropertiesSideEffectStep uniGraphPropertiesSideEffectStep = new UniGraphPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
                TraversalHelper.insertBeforeStep(uniGraphPropertiesSideEffectStep, filterStep, traversal);
            }
        });

//        TraversalHelper.getStepsOfClass(PropertyMapStep.class, traversal).forEach(propertyMapStep -> {
//            UniGraphPropertiesSideEffectStep uniGraphPropertiesSideEffectStep = new UniGraphPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
//            TraversalHelper.insertBeforeStep(uniGraphPropertiesSideEffectStep, propertyMapStep, traversal);
//        });

        TraversalHelper.getStepsOfClass(PropertyValueStep.class, traversal).forEach(propertyValueStep -> {
            if (!(propertyValueStep.getPreviousStep() instanceof UniGraphVertexStepWithProperties)) {
                UniGraphPropertiesSideEffectStep uniGraphPropertiesSideEffectStep = new UniGraphPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
                TraversalHelper.insertBeforeStep(uniGraphPropertiesSideEffectStep, propertyValueStep, traversal);
            }
        });

        TraversalHelper.getStepsOfAssignableClass(MapStep.class, traversal).forEach(mapStep -> {
            if (!(mapStep.getPreviousStep() instanceof UniGraphVertexStepWithProperties)) {
                UniGraphPropertiesSideEffectStep uniGraphPropertiesSideEffectStep = new UniGraphPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
                TraversalHelper.insertBeforeStep(uniGraphPropertiesSideEffectStep, mapStep, traversal);
            }
        });

        TraversalHelper.getStepsOfAssignableClass(ReducingBarrierStep.class, traversal).forEach(reducingBarrierStep -> {
            if (!(reducingBarrierStep.getPreviousStep() instanceof UniGraphVertexStepWithProperties)) {
                if (!(reducingBarrierStep instanceof UniGraphCountStep)) {
                    UniGraphPropertiesSideEffectStep uniGraphPropertiesSideEffectStep = new UniGraphPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
                    TraversalHelper.insertBeforeStep(uniGraphPropertiesSideEffectStep, reducingBarrierStep, traversal);
                }
            }
        });

        TraversalHelper.getStepsOfAssignableClass(SideEffectStep.class, traversal).forEach(sideEffectStep -> {
            if (!(sideEffectStep.getPreviousStep() instanceof UniGraphVertexStepWithProperties)) {
                UniGraphPropertiesSideEffectStep uniGraphPropertiesSideEffectStep = new UniGraphPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
                TraversalHelper.insertBeforeStep(uniGraphPropertiesSideEffectStep, sideEffectStep, traversal);
            }
        });

        Optional<Step> step = TraversalHelper.getLastStepOfAssignableClass(Step.class, traversal);
        if (step.isPresent()) {
            UniGraphPropertiesSideEffectStep uniGraphPropertiesSideEffectStep = new UniGraphPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
            if (!(step.get() instanceof ComputerAwareStep.EndStep) &&
                    !(step.get() instanceof PropertiesStep) &&
                    !(step.get() instanceof HasNextStep) &&
                    !(step.get() instanceof PropertyMapStep) &&
                    !(step.get() instanceof PropertyValueStep) &&
                    !(step.get() instanceof RepeatStep.RepeatEndStep) &&
                    !(step.get() instanceof UniGraphVertexStepWithProperties) &&
                    !(step.get() instanceof UniGraphCountStep))
                TraversalHelper.insertAfterStep(uniGraphPropertiesSideEffectStep, step.get(), traversal);
        }
    }
}
