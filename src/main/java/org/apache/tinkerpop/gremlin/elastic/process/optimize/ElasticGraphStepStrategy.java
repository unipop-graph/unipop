package org.apache.tinkerpop.gremlin.elastic.process.optimize;

import org.apache.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import org.apache.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.*;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.ArrayList;

public class ElasticGraphStepStrategy extends AbstractTraversalStrategy {
    private static final ElasticGraphStepStrategy INSTANCE = new ElasticGraphStepStrategy();
    private ThreadLocal<ElasticGraph> graph = new ThreadLocal<>();

    public static ElasticGraphStepStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if(traversal.getEngine().isComputer()) return;
        if(traversal.getGraph().isPresent() && (traversal.getGraph().get() instanceof ElasticGraph))
            this.graph.set((ElasticGraph) traversal.getGraph().get());
        else return;

        processStep(traversal.getStartStep(), traversal, graph.get().elasticService);

    }

    private void processStep(Step<?, ?> currentStep, Traversal.Admin<?, ?> traversal, ElasticService elasticService) {
        Integer resultsLimit = null;

        if(currentStep instanceof EmptyStep)
            return;
        else if (currentStep instanceof GraphStep) {
            ArrayList<HasContainer> hasContainers = getPredicates(currentStep, traversal);
            final ElasticGraphStep<?> elasticGraphStep = new ElasticGraphStep<>((GraphStep) currentStep, hasContainers, elasticService,resultsLimit);
            TraversalHelper.replaceStep(currentStep, (Step) elasticGraphStep, traversal);
        }
        else if (currentStep instanceof VertexStep) {
            VertexStep vertexStep = (VertexStep) currentStep;
            boolean returnVertex = vertexStep.getReturnClass().equals(Vertex.class);
            ArrayList<HasContainer> hasContainers = returnVertex ? new ArrayList() : getPredicates(currentStep, traversal);

            SearchEdges elasticVertexStep = new SearchEdges((VertexStep<Edge>) currentStep, hasContainers, elasticService, resultsLimit);
            TraversalHelper.replaceStep(currentStep, elasticVertexStep, traversal);
        }

        processStep(currentStep.getNextStep(), traversal, elasticService);
    }

    private ArrayList<HasContainer> getPredicates(Step step, Traversal.Admin traversal){
        ArrayList<HasContainer> hasContainers = new ArrayList<>();
        Step<?, ?> nextStep = step.getNextStep();

        while(nextStep instanceof HasContainerHolder) {
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
                TraversalHelper.insertAfterStep(identityStep, step, traversal);
            }
            if(!containesLambada[0]) traversal.removeStep(nextStep);
            nextStep  = nextStep.getNextStep();
        }
        return hasContainers;
    }
}
