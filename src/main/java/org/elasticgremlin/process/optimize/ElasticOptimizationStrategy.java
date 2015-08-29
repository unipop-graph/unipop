package org.elasticgremlin.process.optimize;

import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.queryhandler.Predicates;
import org.elasticgremlin.structure.ElasticGraph;

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


        TraversalHelper.getStepsOfClass(RepeatStep.class, traversal).forEach(repeatStep -> {
            RepeatBulkStep repeatBulkStep = new RepeatBulkStep<>(traversal, repeatStep);
            TraversalHelper.replaceStep(repeatStep, repeatBulkStep, traversal);
        });

        TraversalHelper.getStepsOfClass(GraphStep.class, traversal).forEach(graphStep -> {
            if(graphStep.getIds().length == 0) {
                Predicates predicates = getPredicates(graphStep, traversal);
                final ElasticGraphStep<?> elasticGraphStep = new ElasticGraphStep<>(graphStep, predicates, elasticGraph.getQueryHandler());
                TraversalHelper.replaceStep(graphStep, (Step) elasticGraphStep, traversal);
            }
        });

        TraversalHelper.getStepsOfClass(VertexStep.class, traversal).forEach(vertexStep -> {
            boolean returnVertex = vertexStep.getReturnClass().equals(Vertex.class);
            Predicates predicates = returnVertex ? new Predicates() : getPredicates(vertexStep, traversal);

            ElasticVertexStep elasticVertexStep = new ElasticVertexStep(vertexStep, predicates, elasticGraph.getQueryHandler());
            TraversalHelper.replaceStep(vertexStep, elasticVertexStep, traversal);
        });

    }

    private Predicates getPredicates(Step step, Traversal.Admin traversal){
        Predicates predicates = new Predicates();
        Step<?, ?> nextStep = step.getNextStep();

        while(true) {
            if(nextStep instanceof HasContainerHolder) {
                HasContainerHolder hasContainerHolder = (HasContainerHolder) nextStep;
                hasContainerHolder.getHasContainers().forEach(predicates.hasContainers::add);
                traversal.removeStep(nextStep);
                if(collectLabels(predicates, nextStep)) return predicates;
            }
            else if(nextStep instanceof RangeGlobalStep) {
                /*RangeGlobalStep rangeGlobalStep = (RangeGlobalStep) nextStep;
                predicates.limitLow = rangeGlobalStep.getLowRange();
                predicates.limitHigh = rangeGlobalStep.getHighRange();
                traversal.removeStep(rangeGlobalStep);
                if(collectLabels(predicates, nextStep)) return predicates;*/
            }
            else return predicates;

            nextStep = nextStep.getNextStep();
        }
    }

    private boolean collectLabels(Predicates predicates, Step<?, ?> step) {
        step.getLabels().forEach(predicates.labels::add);
        return step.getLabels().size() > 0;
    }
}
