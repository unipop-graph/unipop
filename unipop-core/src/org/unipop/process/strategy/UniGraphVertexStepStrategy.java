package org.unipop.process.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.controller.Predicates;
import org.unipop.process.UniGraphCountStep;
import org.unipop.process.UniGraphPropertiesSideEffectStep;
import org.unipop.process.UniGraphVertexStep;
import org.unipop.process.UniGraphVertexStepWithProperties;
import org.unipop.structure.UniGraph;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Created by Roman on 11/12/2015.
 */
public class UniGraphVertexStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    private boolean instanceOfAny(Step step, Class... classes) {
        for (Class aClass : classes) {
            if (aClass.isAssignableFrom(step.getClass()))
                return true;
        }
        return false;
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

        TraversalHelper.getStepsOfClass(VertexStep.class, traversal).forEach(vertexStep -> {
            UniGraphVertexStep uniGraphVertexStep;
            if (instanceOfAny(vertexStep.getNextStep(), PropertiesStep.class, HasStep.class, FilterStep.class,
                    PropertyValueStep.class, MapStep.class, ReducingBarrierStep.class, SideEffectStep.class) && false) {
                uniGraphVertexStep = new UniGraphVertexStepWithProperties(vertexStep, new Predicates(), uniGraph.getControllerManager());
            } else
                uniGraphVertexStep = new UniGraphVertexStep(vertexStep, new Predicates(), uniGraph.getControllerManager());
            TraversalHelper.replaceStep(vertexStep, uniGraphVertexStep, traversal);
        });

//        Optional<UniGraphVertexStep> lastUniGraphVertexStep = TraversalHelper.getLastStepOfAssignableClass(UniGraphVertexStep.class, traversal);
//        if (lastUniGraphVertexStep.isPresent() && !(lastUniGraphVertexStep.get() instanceof UniGraphVertexStepWithProperties)) {
//            UniGraphVertexStep uniGraphVertexStep = lastUniGraphVertexStep.get();
//            TraversalHelper.replaceStep(uniGraphVertexStep, new UniGraphVertexStepWithProperties(uniGraphVertexStep), traversal);
//        }

    }
}
