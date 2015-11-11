package org.unipop.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.process.UniGraphCountStep;
import org.unipop.process.UniGraphStartStep;
import org.unipop.process.UniGraphVertexStep;
import org.unipop.structure.UniGraph;

import java.util.*;

/**
 * Created by Gilad on 02/11/2015.
 */
public class UniGraphCountStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.VendorOptimizationStrategy> implements TraversalStrategy.VendorOptimizationStrategy {
    //region AbstractTraversalStrategy Implementation
    @Override
    public Set<Class<? extends TraversalStrategy.VendorOptimizationStrategy>> applyPrior() {
        Set<Class<? extends TraversalStrategy.VendorOptimizationStrategy>> priorStrategies = new HashSet<>();
        priorStrategies.add(ElasticPredicatesGatheringStrategy.class);
        return priorStrategies;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if(traversal.getEngine().isComputer()) return;

        Graph graph = traversal.getGraph().get();
        if(!(graph instanceof UniGraph)) return;

        UniGraph elasticGraph = (UniGraph) graph;

        TraversalHelper.getStepsOfAssignableClassRecursively(CountGlobalStep.class, traversal).forEach(step -> {
            UniGraphCountStep elasticCountStep = null;
            if (UniGraphVertexStep.class.isAssignableFrom(step.getPreviousStep().getClass())) {
                UniGraphVertexStep elasticVertexStep = (UniGraphVertexStep)step.getPreviousStep();
                elasticCountStep = new UniGraphCountStep(
                        traversal,
                        elasticVertexStep.getReturnClass(),
                        elasticVertexStep.getPredicates(),
                        new Object[0],
                        elasticVertexStep.getEdgeLabels(),
                        Optional.of(elasticVertexStep.getDirection()), elasticGraph.getControllerManager());

            } else if (UniGraphStartStep.class.isAssignableFrom(step.getPreviousStep().getClass())) {
                UniGraphStartStep elasticGraphStep = (UniGraphStartStep)step.getPreviousStep();
                elasticCountStep = new UniGraphCountStep(
                        traversal,
                        elasticGraphStep.getReturnClass(),
                        elasticGraphStep.getPredicates(),
                        elasticGraphStep.getIds(),
                        new String[0],
                        Optional.empty(), elasticGraph.getControllerManager());
            }

            if (elasticCountStep != null) {
                TraversalHelper.replaceStep(step.getPreviousStep(), elasticCountStep, traversal);
                traversal.removeStep(step);

                insertDummyStepWhenTraversalIsInternal(traversal, elasticCountStep);
            }
        });
    }
    //endregion

    //region Private Methods
    private void insertDummyStepWhenTraversalIsInternal(final Traversal.Admin<?, ?> traversal, Step step) {
        if (!traversal.getParent().equals(EmptyStep.instance())) {
            SideEffectStep dummyStep = new SideEffectStep(traversal) {
                @Override
                protected void sideEffect(Traverser.Admin admin) {
                    //do nothing
                }

                @Override
                public String toString() {
                    return "DummyStep";
                }
            };

            TraversalHelper.insertBeforeStep(dummyStep, step, traversal);
        }
    }
    //endregion
}
