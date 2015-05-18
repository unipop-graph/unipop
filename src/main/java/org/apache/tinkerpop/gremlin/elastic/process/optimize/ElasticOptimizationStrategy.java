package org.apache.tinkerpop.gremlin.elastic.process.optimize;

import org.apache.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.*;
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
                ArrayList<HasContainer> hasContainers = getPredicates(graphStep, traversal);
                final ElasticGraphStep<?> elasticGraphStep = new ElasticGraphStep<>(graphStep, hasContainers, graph.get().elasticService, null);
                TraversalHelper.replaceStep(graphStep, (Step) elasticGraphStep, traversal);
            }
        });

        TraversalHelper.getStepsOfClass(VertexStep.class, traversal).forEach(vertexStep -> {
            boolean returnVertex = vertexStep.getReturnClass().equals(Vertex.class);
            ArrayList<HasContainer> hasContainers = returnVertex ? new ArrayList() : getPredicates(vertexStep, traversal);

            ElasticVertexStep elasticVertexStep = new ElasticVertexStep(vertexStep, hasContainers, graph.get().elasticService, null);
            TraversalHelper.replaceStep(vertexStep, elasticVertexStep, traversal);
        });
    }

    private ArrayList<HasContainer> getPredicates(Step step, Traversal.Admin traversal){
        ArrayList<HasContainer> hasContainers = new ArrayList<>();
        Step<?, ?> nextStep = step.getNextStep();

        while(nextStep instanceof HasContainerHolder) {
            if(nextStep instanceof LambdaHolder)
                continue;
            ((HasContainerHolder) nextStep).getHasContainers().forEach((has)-> hasContainers.add(has));

            if (nextStep.getLabels().size() > 0) { //if the step containes as("label")
                final IdentityStep identityStep = new IdentityStep<>(traversal);
                nextStep.getLabels().forEach(label -> identityStep.addLabel(label.toString()));
                TraversalHelper.insertAfterStep(identityStep, step, traversal);
            }
            traversal.removeStep(nextStep);
            nextStep  = nextStep.getNextStep();
        }
        return hasContainers;
    }
}
