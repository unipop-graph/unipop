package org.unipop.process.properties;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.process.count.UniGraphCountStepStrategy;
import org.unipop.process.predicate.UniGraphPredicatesStrategy;
import org.unipop.process.start.UniGraphStartStepStrategy;
import org.unipop.process.group.UniGraphGroupCountStepStrategy;
import org.unipop.process.group.UniGraphGroupStepStrategy;
import org.unipop.process.vertex.UniGraphVertexStepStrategy;
import org.unipop.structure.UniGraph;

import java.util.Set;


/**
 * Created by sbarzilay on 3/9/16.
 */
public class UniGraphPropertiesStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        return Sets.newHashSet(UniGraphStartStepStrategy.class,
                UniGraphVertexStepStrategy.class,
                UniGraphPredicatesStrategy.class,
                UniGraphGroupStepStrategy.class,
                UniGraphGroupCountStepStrategy.class,
                UniGraphCountStepStrategy.class);
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
            UniGraphPropertiesSideEffectStep uniGraphPropertiesSideEffectStep = new UniGraphPropertiesSideEffectStep(traversal, uniGraph.getControllerProvider());
            TraversalHelper.insertBeforeStep(uniGraphPropertiesSideEffectStep, propertiesStep, traversal);
        });

        TraversalHelper.getStepsOfClass(PropertyValueStep.class, traversal).forEach(propertyValueStep -> {
            UniGraphPropertiesSideEffectStep uniGraphPropertiesSideEffectStep = new UniGraphPropertiesSideEffectStep(traversal, uniGraph.getControllerProvider());
            TraversalHelper.insertBeforeStep(uniGraphPropertiesSideEffectStep, propertyValueStep, traversal);
        });
        TraversalHelper.getStepsOfClass(PropertyMapStep.class, traversal).forEach(propertyValueStep -> {
            UniGraphPropertiesSideEffectStep uniGraphPropertiesSideEffectStep = new UniGraphPropertiesSideEffectStep(traversal, uniGraph.getControllerProvider());
            TraversalHelper.insertBeforeStep(uniGraphPropertiesSideEffectStep, propertyValueStep, traversal);
        });

        TraversalHelper.getStepsOfClass(HasStep.class, traversal).forEach(hasStep -> {
            UniGraphPropertiesSideEffectStep uniGraphPropertiesSideEffectStep = new UniGraphPropertiesSideEffectStep(traversal, uniGraph.getControllerProvider());
            TraversalHelper.insertBeforeStep(uniGraphPropertiesSideEffectStep, hasStep, traversal);
        });

        TraversalHelper.getStepsOfAssignableClass(TraversalParent.class, traversal).forEach(step -> {
            step.getLocalChildren().stream().filter(child -> child instanceof ElementValueTraversal).findAny().ifPresent( x -> {
                UniGraphPropertiesSideEffectStep uniGraphPropertiesSideEffectStep = new UniGraphPropertiesSideEffectStep(traversal, uniGraph.getControllerProvider());
                TraversalHelper.insertBeforeStep(uniGraphPropertiesSideEffectStep, (Step) step, traversal);
            });
        });
//
//        TraversalHelper.getStepsOfAssignableClass(FilterStep.class, traversal).forEach(filterStep -> {
//            UniGraphPropertiesSideEffectStep uniGraphPropertiesSideEffectStep = new UniGraphPropertiesSideEffectStep(traversal, uniGraph.getControllerProvider());
//            TraversalHelper.insertBeforeStep(uniGraphPropertiesSideEffectStep, filterStep, traversal);
//        });

//        TraversalHelper.getStepsOfClass(PropertyMapStep.class, traversal).forEach(propertyMapStep -> {
//            UniGraphPropertiesSideEffectStep uniGraphPropertiesSideEffectStep = new UniGraphPropertiesSideEffectStep(traversal, uniGraph.getControllerProvider());
//            TraversalHelper.insertBeforeStep(uniGraphPropertiesSideEffectStep, propertyMapStep, traversal);
//        });


//        TraversalHelper.getStepsOfAssignableClass(MapStep.class, traversal).forEach(mapStep -> {
//            UniGraphPropertiesSideEffectStep uniGraphPropertiesSideEffectStep = new UniGraphPropertiesSideEffectStep(traversal, uniGraph.getControllerProvider());
//            TraversalHelper.insertBeforeStep(uniGraphPropertiesSideEffectStep, mapStep, traversal);
//        });
//
//        TraversalHelper.getStepsOfAssignableClass(ReducingBarrierStep.class, traversal).forEach(reducingBarrierStep -> {
//            UniGraphPropertiesSideEffectStep uniGraphPropertiesSideEffectStep = new UniGraphPropertiesSideEffectStep(traversal, uniGraph.getControllerProvider());
//            TraversalHelper.insertBeforeStep(uniGraphPropertiesSideEffectStep, reducingBarrierStep, traversal);
//        });
//
//        TraversalHelper.getStepsOfAssignableClass(SideEffectStep.class, traversal).forEach(sideEffectStep -> {
//            UniGraphPropertiesSideEffectStep uniGraphPropertiesSideEffectStep = new UniGraphPropertiesSideEffectStep(traversal, uniGraph.getControllerProvider());
//            TraversalHelper.insertBeforeStep(uniGraphPropertiesSideEffectStep, sideEffectStep, traversal);
//        });

        Step step = TraversalHelper.getLastStepOfAssignableClass(Step.class, traversal).get();
        UniGraphPropertiesSideEffectStep uniGraphPropertiesSideEffectStep = new UniGraphPropertiesSideEffectStep(traversal, uniGraph.getControllerProvider());
        if (!(step instanceof ComputerAwareStep.EndStep) &&
                !(step instanceof PropertiesStep) &&
                !(step instanceof HasNextStep) &&
                !(step instanceof PropertyMapStep) &&
                !(step instanceof PropertyValueStep))
            TraversalHelper.insertAfterStep(uniGraphPropertiesSideEffectStep, step, traversal);
    }
}
