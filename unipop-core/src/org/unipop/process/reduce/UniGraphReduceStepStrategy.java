package org.unipop.process.reduce;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.process.predicate.PredicatesUtil;
import org.unipop.process.start.UniGraphStartStep;
import org.unipop.process.vertex.UniGraphVertexStep;
import org.unipop.query.aggregation.reduce.ReduceQuery;
import org.unipop.structure.UniGraph;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * @author Gur Ronen
 * @since 6/28/2016
 */
public class UniGraphReduceStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy  {
    //region AbstractTraversalStrategy Implementation
    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        Set<Class<? extends TraversalStrategy.ProviderOptimizationStrategy>> priorStrategies = new HashSet<>();
        priorStrategies.add(PredicatesUtil.class);
        return priorStrategies;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if(traversal.getEngine().isComputer()) return;

        Graph graph = traversal.getGraph().get();
        if(!(graph instanceof UniGraph)) return;

        UniGraph uniGraph = (UniGraph) graph;

        TraversalHelper.getStepsOfAssignableClassRecursively(CountGlobalStep.class, traversal).forEach(step -> {
            UniGraphReduceStep uniReduceStep = null;
            if (UniGraphVertexStep.class.isAssignableFrom(step.getPreviousStep().getClass())) {
                UniGraphVertexStep uniVertexStep = (UniGraphVertexStep)step.getPreviousStep();
                uniReduceStep = new UniGraphReduceStep(
                        traversal,
                        uniVertexStep.getReturnClass(),
                        uniGraph.getControllerManager(),
                        ,
                        ReduceQuery.Op.COUNT
                );

            } else if (UniGraphStartStep.class.isAssignableFrom(step.getPreviousStep().getClass())) {
                UniGraphStartStep elasticGraphStep = (UniGraphStartStep)step.getPreviousStep();
                uniReduceStep = new UniGraphCountStep(
                        traversal,
                        elasticGraphStep.getReturnClass(),
                        elasticGraphStep.getPredicates(),
                        elasticGraphStep.getIds(),
                        new String[0],
                        Optional.empty(), uniGraph.getControllerManager());
            }

            if (uniReduceStep != null) {
                TraversalHelper.replaceStep(step.getPreviousStep(), uniReduceStep, traversal);
                traversal.removeStep(step);

                insertStartStepWhenTraversalIsInternal(traversal, uniReduceStep);
            }
        });
    }
    //endregion

    //region Private Methods
    private void insertStartStepWhenTraversalIsInternal(final Traversal.Admin<?, ?> traversal, Step step) {
        if (!traversal.getParent().equals(EmptyStep.instance())) {
            StartStep startStep = new StartStep(traversal);
            TraversalHelper.insertBeforeStep(startStep, step, traversal);
        }
    }
    //endregion

}
