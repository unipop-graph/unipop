package org.apache.tinkerpop.gremlin.elastic.process.graph.traversal.strategy;

import org.apache.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import org.apache.tinkerpop.gremlin.elastic.process.graph.traversal.steps.ElasticGraphStep;
import org.apache.tinkerpop.gremlin.elastic.process.graph.traversal.steps.flatmap.*;
import org.apache.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.*;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.ArrayList;

public class ElasticGraphStepStrategy extends AbstractTraversalStrategy {
    private static final ElasticGraphStepStrategy INSTANCE = new ElasticGraphStepStrategy();

    public static ElasticGraphStepStrategy instance() {
        return INSTANCE;
    }
    private static ThreadLocal<ElasticGraph> graph = new ThreadLocal<>();


    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if (traversal.getEngine().isComputer()) return;

        Step<?, ?> startStep = traversal.getStartStep();
        if(startStep instanceof GraphStep || graph != null) {
            if(startStep instanceof GraphStep ) graph.set((ElasticGraph) traversal.getGraph().get());
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
        /*if(nextStep instanceof RangeStep){
           RangeStep rangeStep = (RangeStep) nextStep;
            if (isLimitStep(rangeStep) && stepCanConsumePredicates(currentStep)){
                resultsLimit = (int) rangeStep.getHighRange();
                traversal.removeStep(nextStep);
            }
        }*/

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
        /*else if (currentStep instanceof RepeatStep){
            RepeatStep originalRepeatStep = (RepeatStep) currentStep;
            ElasticRepeatStep repeatStep = new ElasticRepeatStep(currentStep.getTraversal(),originalRepeatStep);
            TraversalHelper.replaceStep(currentStep, (Step) repeatStep, traversal);
        }
        else if (currentStep instanceof LocalStep){
            //local step is working on each vertex -> we don't want our strategy to apply on this step
            LocalStep localStep = (LocalStep) currentStep;
            ((Traversal) localStep.getTraversals().get(0)).asAdmin().setStrategies(TraversalStrategies.GlobalCache.getStrategies(Graph.class));
        }
        else if (currentStep instanceof UnionStep){
            UnionStep originalUnionStep = (UnionStep) currentStep;
            ElasticUnionStep unionStep = new ElasticUnionStep(currentStep.getTraversal(),originalUnionStep);
            TraversalHelper.replaceStep(currentStep, (Step) unionStep, traversal);
        }*/
        else {
            //TODO
        }

        if(!(currentStep instanceof EmptyStep)) processStep(nextStep, traversal, elasticService);
    }

    private boolean stepCanConsumePredicates(Step<?, ?> step) {
        return (step instanceof VertexStep)|| (step instanceof EdgeVertexStep ) || (step instanceof GraphStep);
    }

    /*private boolean isLimitStep(RangeStep rangeStep) {
        return rangeStep.getLowRange() == 0;
    }*/

}
