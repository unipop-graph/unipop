package org.unipop.process.reduce;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.process.predicate.PredicatesUtil;
import org.unipop.process.reduce.ops.Op;
import org.unipop.process.reduce.ops.OpFactory;
import org.unipop.process.start.UniGraphStartStep;
import org.unipop.process.start.UniGraphStartStepStrategy;
import org.unipop.process.vertex.UniGraphVertexStep;
import org.unipop.process.vertex.UniGraphVertexStepStrategy;
import org.unipop.query.aggregation.reduce.ReduceQuery;
import org.unipop.structure.UniGraph;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Gur Ronen
 * @since 6/28/2016
 */
public class UniGraphReduceStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy  {
    //region AbstractTraversalStrategy Implementation
    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        return Stream.of(UniGraphStartStepStrategy.class, UniGraphVertexStepStrategy.class).collect(Collectors.toSet());
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
//        if(traversal.getEngine().isComputer()) return;

        Graph graph = traversal.getGraph().get();
        if(!(graph instanceof UniGraph)) return;

        UniGraph uniGraph = (UniGraph) graph;

        StreamSupport.stream(new OpFactory().getOps().spliterator(), false).forEach(op -> {
            replaceStep(traversal, uniGraph, op);
        });

    }

    //region Private Methods
    private void insertStartStepWhenTraversalIsInternal(final Traversal.Admin<?, ?> traversal, Step step) {
        if (!traversal.getParent().equals(EmptyStep.instance())) {
            StartStep startStep = new StartStep(traversal);
            TraversalHelper.insertBeforeStep(startStep, step, traversal);
        }
    }

    private void replaceStep(Traversal.Admin<?, ?> traversal, UniGraph uniGraph, Op reductionOperator) {
        TraversalHelper.getStepsOfAssignableClassRecursively(reductionOperator.getStepToReplace(), traversal).forEach(step -> {
            UniGraphReduceStep uniReduceStep = null;
            if (UniGraphVertexStep.class.isAssignableFrom(step.getPreviousStep().getClass())) {
                UniGraphVertexStep uniVertexStep = (UniGraphVertexStep)step.getPreviousStep();
                uniReduceStep = new UniGraphReduceStep(
                        traversal,
                        uniVertexStep.getReturnClass(),
                        uniGraph.getControllerManager(),
                        reductionOperator
                );

            } else if (UniGraphStartStep.class.isAssignableFrom(step.getPreviousStep().getClass())) {
                UniGraphStartStep uniGraphStartStep = (UniGraphStartStep)step.getPreviousStep();
                uniReduceStep = new UniGraphReduceStep(
                        traversal,
                        uniGraphStartStep.getReturnClass(),
                        uniGraph.getControllerManager(),
                        reductionOperator
                );
            }

            if (uniReduceStep != null) {
                TraversalHelper.replaceStep(step.getPreviousStep(), uniReduceStep, traversal);
                traversal.removeStep(step);

                insertStartStepWhenTraversalIsInternal(traversal, uniReduceStep);
            }
        });
    }
    //endregion
}
