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
import org.apache.tinkerpop.gremlin.structure.*;

public class ElasticOptimizationStrategy extends AbstractTraversalStrategy<TraversalStrategy.VendorOptimizationStrategy> {
    private static final ElasticOptimizationStrategy INSTANCE = new ElasticOptimizationStrategy();
    public static ElasticOptimizationStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if(traversal.getEngine().isComputer()) return;
        Graph graph = traversal.getGraph().get();
        if(!(graph instanceof ElasticGraph)) return;
        ElasticGraph elasticGraph = (ElasticGraph) graph;

        TraversalHelper.getStepsOfClass(GraphStep.class, traversal).forEach(graphStep -> {
            if(graphStep.getIds().length == 0) {
                Predicates predicates = getPredicates(graphStep, traversal);
                final ElasticGraphStep<?> elasticGraphStep = new ElasticGraphStep<>(graphStep, predicates, elasticGraph.elasticService);
                TraversalHelper.replaceStep(graphStep, (Step) elasticGraphStep, traversal);
            }
        });

        TraversalHelper.getStepsOfClass(VertexStep.class, traversal).forEach(vertexStep -> {
            boolean returnVertex = vertexStep.getReturnClass().equals(Vertex.class);
            Predicates predicates = returnVertex ? new Predicates() : getPredicates(vertexStep, traversal);

            ElasticVertexStep elasticVertexStep = new ElasticVertexStep(vertexStep, predicates, elasticGraph.elasticService);
            TraversalHelper.replaceStep(vertexStep, elasticVertexStep, traversal);
        });
    }

    private Predicates getPredicates(Step step, Traversal.Admin traversal){
        Predicates predicates = new Predicates();
        Step<?, ?> nextStep = step.getNextStep();

        while(true) {
            if(nextStep instanceof HasContainerHolder) {
                HasContainerHolder hasContainerHolder = (HasContainerHolder) nextStep;
                boolean skip = false;
                for(HasContainer has : hasContainerHolder.getHasContainers())
                    if(has.getPredicate().getTraversals().size() > 0)
                        skip = true;

                if(!skip) {
                    hasContainerHolder.getHasContainers().forEach((has)-> predicates.hasContainers.add(has));
                    nextStep.getLabels().forEach(label -> predicates.labels.add(label.toString()));
                    traversal.removeStep(nextStep);
                }
            }
            else if(nextStep instanceof RangeGlobalStep){
                RangeGlobalStep rangeGlobalStep = (RangeGlobalStep) nextStep;
                predicates.limitLow = rangeGlobalStep.getLowRange();
                predicates.limitHigh = rangeGlobalStep.getHighRange();
                traversal.removeStep(nextStep);
            }
            else return predicates;

            nextStep  = nextStep.getNextStep();
        }
    }
}
