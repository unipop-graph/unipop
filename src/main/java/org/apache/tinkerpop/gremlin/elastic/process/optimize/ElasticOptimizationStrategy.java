package org.apache.tinkerpop.gremlin.elastic.process.optimize;

import org.apache.tinkerpop.gremlin.elastic.elasticservice.Predicates;
import org.apache.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;

public class ElasticOptimizationStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> {
    private static final ElasticOptimizationStrategy INSTANCE = new ElasticOptimizationStrategy();
    private ThreadLocal<ElasticGraph> graph = new ThreadLocal<>();

    public static ElasticOptimizationStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if(traversal.getEngine().isComputer()) return;
        if(traversal.getGraph().isPresent() && (traversal.getGraph().get() instanceof ElasticGraph))
            this.graph.set((ElasticGraph) traversal.getGraph().get());
        else return;

        TraversalHelper.getStepsOfClass(GraphStep.class, traversal).forEach(graphStep -> {
            if(graphStep.getIds().length == 0) {
                Predicates predicates = getPredicates(graphStep, traversal);
                final ElasticGraphStep<?> elasticGraphStep = new ElasticGraphStep<>(graphStep, predicates, graph.get().elasticService);
                TraversalHelper.replaceStep(graphStep, (Step) elasticGraphStep, traversal);
            }
        });

        TraversalHelper.getStepsOfClass(VertexStep.class, traversal).forEach(vertexStep -> {
            boolean returnVertex = vertexStep.getReturnClass().equals(Vertex.class);
            Predicates predicates = returnVertex ? new Predicates() : getPredicates(vertexStep, traversal);

            ElasticVertexStep elasticVertexStep = new ElasticVertexStep(vertexStep, predicates, graph.get().elasticService);
            TraversalHelper.replaceStep(vertexStep, elasticVertexStep, traversal);
        });
    }

    private Predicates getPredicates(Step step, Traversal.Admin traversal){
        Predicates predicates = new Predicates();
        Step<?, ?> nextStep = step.getNextStep();

        while(true) {
            if(nextStep instanceof LambdaHolder){
                //do nothing
            }
            else if(nextStep instanceof HasContainerHolder) {
                ((HasContainerHolder) nextStep).getHasContainers().forEach((has)->
                        predicates.hasContainers.add(has));
                nextStep.getLabels().forEach(label -> predicates.labels.add(label.toString()));
                traversal.removeStep(nextStep);
            }
            else if(nextStep instanceof RangeGlobalStep){
                RangeGlobalStep rangeGlobalStep = (RangeGlobalStep) nextStep;
                predicates.limitLow = rangeGlobalStep.getLowRange();
                predicates.limitHigh = rangeGlobalStep.getHighRange();
                nextStep.getLabels().forEach(label -> predicates.labels.add(label.toString()));
                traversal.removeStep(nextStep);
            }
            else return predicates;

            nextStep  = nextStep.getNextStep();
        }
    }
}
