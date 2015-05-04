package com.tinkerpop.gremlin.elastic.process.graph.traversal.strategy;

import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import com.tinkerpop.gremlin.elastic.process.graph.traversal.steps.*;
import com.tinkerpop.gremlin.elastic.process.graph.traversal.steps.flatmap.*;
import com.tinkerpop.gremlin.elastic.process.graph.traversal.steps.traversalHolder.ElasticRepeatStep;
import com.tinkerpop.gremlin.elastic.process.graph.traversal.steps.traversalHolder.ElasticUnionStep;
import com.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import com.tinkerpop.gremlin.process.*;
import com.tinkerpop.gremlin.process.graph.marker.HasContainerHolder;
import com.tinkerpop.gremlin.process.graph.step.branch.RepeatStep;
import com.tinkerpop.gremlin.process.graph.step.branch.UnionStep;
import com.tinkerpop.gremlin.process.graph.step.filter.RangeStep;
import com.tinkerpop.gremlin.process.graph.step.map.*;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.strategy.AbstractTraversalStrategy;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.process.util.*;
import com.tinkerpop.gremlin.structure.*;

import java.util.*;

public class ElasticGraphStepStrategy extends AbstractTraversalStrategy {
    private static final ElasticGraphStepStrategy INSTANCE = new ElasticGraphStepStrategy();

    public static ElasticGraphStepStrategy instance() {
        return INSTANCE;
    }
    private static ThreadLocal<ElasticGraph> graph = new ThreadLocal<>();

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.COMPUTER)) return;

        Step<?, ?> startStep = TraversalHelper.getStart(traversal);
        if(startStep instanceof GraphStep || graph != null) {
            if(startStep instanceof GraphStep ) graph.set((ElasticGraph) ((GraphStep) startStep).getGraph(ElasticGraph.class));
            processStep(startStep, traversal, graph.get().elasticService);
        }

    }

    private void processStep(Step<?, ?> currentStep, Traversal.Admin<?, ?> traversal, ElasticService elasticService) {
        ArrayList<HasContainer> hasContainers = new ArrayList<>();
        Step<?, ?> nextStep = currentStep.getNextStep();

        while(stepCanConsumePredicates(currentStep) && nextStep instanceof HasContainerHolder) {
            final boolean[] containesLambada = {false};
            ((HasContainerHolder) nextStep).getHasContainers().forEach((has)->{
                if(has.predicate.toString().contains("$$")){
                    containesLambada[0] = true;
                }
                else hasContainers.add(has);
            });
            //if the step containes as("label")
            if (nextStep.getLabel().isPresent()) {
                final IdentityStep identityStep = new IdentityStep<>(traversal);
                identityStep.setLabel(nextStep.getLabel().get());
                TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
            }
            if(!containesLambada[0]) traversal.removeStep(nextStep);
            nextStep  = nextStep.getNextStep();
        }

        Integer resultsLimit = null;
        if(nextStep instanceof RangeStep){
           RangeStep rangeStep = (RangeStep) nextStep;
            if (isLimitStep(rangeStep) && stepCanConsumePredicates(currentStep)){
                resultsLimit = (int) rangeStep.getHighRange();
                traversal.removeStep(nextStep);
            }
        }

        if (currentStep instanceof GraphStep) {
            final ElasticGraphStep<?> elasticGraphStep = new ElasticGraphStep<>((GraphStep) currentStep, hasContainers, elasticService,resultsLimit);
            TraversalHelper.replaceStep(currentStep, (Step) elasticGraphStep, traversal);
        }
        else if (currentStep instanceof VertexStep) {
            ElasticVertexStep<Element> elasticVertexStep = new ElasticVertexStep<>((VertexStep) currentStep, hasContainers, elasticService,resultsLimit);
            TraversalHelper.replaceStep(currentStep, (Step) elasticVertexStep, traversal);
        }
        else if (currentStep instanceof EdgeVertexStep){
            ElasticEdgeVertexStep newSearchStep = new ElasticEdgeVertexStep((EdgeVertexStep)currentStep, hasContainers, elasticService,resultsLimit);
            TraversalHelper.replaceStep(currentStep, (Step) newSearchStep, traversal);
        }
        else if (currentStep instanceof RepeatStep){
            RepeatStep originalRepeatStep = (RepeatStep) currentStep;
            ElasticRepeatStep repeatStep = new ElasticRepeatStep(currentStep.getTraversal(),originalRepeatStep);
            TraversalHelper.replaceStep(currentStep, (Step) repeatStep, traversal);
        }
        else if (currentStep instanceof  LocalStep){
            //local step is working on each vertex -> we don't want our strategy to apply on this step
            LocalStep localStep = (LocalStep) currentStep;
            ((Traversal) localStep.getTraversals().get(0)).asAdmin().setStrategies(TraversalStrategies.GlobalCache.getStrategies(Graph.class));
        }
        else if (currentStep instanceof UnionStep){
            UnionStep originalUnionStep = (UnionStep) currentStep;
            ElasticUnionStep unionStep = new ElasticUnionStep(currentStep.getTraversal(),originalUnionStep);
            TraversalHelper.replaceStep(currentStep, (Step) unionStep, traversal);
        }
        else {
            //TODO
        }

        if(!(currentStep instanceof EmptyStep)) processStep(nextStep, traversal, elasticService);
    }

    private boolean stepCanConsumePredicates(Step<?, ?> step) {
        return (step instanceof VertexStep)|| (step instanceof EdgeVertexStep ) || (step instanceof GraphStep);
    }

    private boolean isLimitStep(RangeStep rangeStep) {
        return rangeStep.getLowRange() == 0;
    }
}
