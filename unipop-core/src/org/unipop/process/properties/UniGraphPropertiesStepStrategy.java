package org.unipop.process.properties;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.process.start.UniGraphStartStepStrategy;
import org.unipop.process.vertex.UniGraphVertexStepStrategy;
import org.unipop.structure.UniGraph;

import java.util.Set;


public class UniGraphPropertiesStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        return Sets.newHashSet(UniGraphStartStepStrategy.class, UniGraphVertexStepStrategy.class);
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if(TraversalHelper.onGraphComputer(traversal)) return;

        Graph graph = traversal.getGraph().get();
        if (!(graph instanceof UniGraph)) {
            return;
        }

        UniGraph uniGraph = (UniGraph) graph;


        TraversalHelper.getStepsOfClass(PropertiesStep.class, traversal).forEach(propertiesStep -> {
            UniGraphVertexPropertiesSideEffectStep uniGraphVertexPropertiesSideEffectStep = new UniGraphVertexPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
            TraversalHelper.insertBeforeStep(uniGraphVertexPropertiesSideEffectStep, propertiesStep, traversal);
        });

        TraversalHelper.getStepsOfClass(PropertyValueStep.class, traversal).forEach(propertyValueStep -> {
            UniGraphVertexPropertiesSideEffectStep uniGraphVertexPropertiesSideEffectStep = new UniGraphVertexPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
            TraversalHelper.insertBeforeStep(uniGraphVertexPropertiesSideEffectStep, propertyValueStep, traversal);
        });
        TraversalHelper.getStepsOfClass(PropertyMapStep.class, traversal).forEach(propertyValueStep -> {
            UniGraphVertexPropertiesSideEffectStep uniGraphVertexPropertiesSideEffectStep = new UniGraphVertexPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
            TraversalHelper.insertBeforeStep(uniGraphVertexPropertiesSideEffectStep, propertyValueStep, traversal);
        });

        TraversalHelper.getStepsOfClass(HasStep.class, traversal).forEach(hasStep -> {
            UniGraphVertexPropertiesSideEffectStep uniGraphVertexPropertiesSideEffectStep = new UniGraphVertexPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
            TraversalHelper.insertBeforeStep(uniGraphVertexPropertiesSideEffectStep, hasStep, traversal);
        });

        TraversalHelper.getStepsOfAssignableClass(TraversalParent.class, traversal).forEach(step -> {
            step.getLocalChildren().stream().filter(child -> child instanceof ElementValueTraversal).findAny().ifPresent( x -> {
                UniGraphVertexPropertiesSideEffectStep uniGraphVertexPropertiesSideEffectStep = new UniGraphVertexPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
                TraversalHelper.insertBeforeStep(uniGraphVertexPropertiesSideEffectStep, (Step) step, traversal);
            });
        });
        
        TraversalHelper.getStepsOfAssignableClass(FilterStep.class, traversal).forEach(filterStep -> {
            UniGraphVertexPropertiesSideEffectStep uniGraphVertexPropertiesSideEffectStep = new UniGraphVertexPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
            TraversalHelper.insertBeforeStep(uniGraphVertexPropertiesSideEffectStep, filterStep, traversal);
        });

        TraversalHelper.getStepsOfClass(PropertyMapStep.class, traversal).forEach(propertyMapStep -> {
            UniGraphVertexPropertiesSideEffectStep uniGraphVertexPropertiesSideEffectStep = new UniGraphVertexPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
            TraversalHelper.insertBeforeStep(uniGraphVertexPropertiesSideEffectStep, propertyMapStep, traversal);
        });


        TraversalHelper.getStepsOfAssignableClass(MapStep.class, traversal).forEach(mapStep -> {
            UniGraphVertexPropertiesSideEffectStep uniGraphVertexPropertiesSideEffectStep = new UniGraphVertexPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
            TraversalHelper.insertBeforeStep(uniGraphVertexPropertiesSideEffectStep, mapStep, traversal);
        });

        TraversalHelper.getStepsOfAssignableClass(ReducingBarrierStep.class, traversal).forEach(reducingBarrierStep -> {
            UniGraphVertexPropertiesSideEffectStep uniGraphVertexPropertiesSideEffectStep = new UniGraphVertexPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
            TraversalHelper.insertBeforeStep(uniGraphVertexPropertiesSideEffectStep, reducingBarrierStep, traversal);
        });

        TraversalHelper.getStepsOfAssignableClass(SideEffectStep.class, traversal).forEach(sideEffectStep -> {
            UniGraphVertexPropertiesSideEffectStep uniGraphVertexPropertiesSideEffectStep = new UniGraphVertexPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
            TraversalHelper.insertBeforeStep(uniGraphVertexPropertiesSideEffectStep, sideEffectStep, traversal);
        });

//        Step step = TraversalHelper.getLastStepOfAssignableClass(Step.class, traversal).get();
//        UniGraphVertexPropertiesSideEffectStep uniGraphVertexPropertiesSideEffectStep = new UniGraphVertexPropertiesSideEffectStep(traversal, uniGraph.getControllerManager());
//        if (!(step instanceof ComputerAwareStep.EndStep) &&
//                !(step instanceof PropertiesStep) &&
//                !(step instanceof HasNextStep) &&
//                !(step instanceof PropertyMapStep) &&
//                !(step instanceof PropertyValueStep))
//            TraversalHelper.insertAfterStep(uniGraphVertexPropertiesSideEffectStep, step, traversal);
    }
}
