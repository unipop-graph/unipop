package com.tinkerpop.gremlin.elastic.process.graph.traversal.strategy;

import com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect.ElasticGraphStep;
import com.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import com.tinkerpop.gremlin.elastic.structure.ElasticVertex;
import com.tinkerpop.gremlin.process.*;
import com.tinkerpop.gremlin.process.graph.marker.HasContainerHolder;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeVertexStep;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.*;
import com.tinkerpop.gremlin.process.graph.strategy.AbstractTraversalStrategy;
import com.tinkerpop.gremlin.process.util.EmptyStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Direction;

public class ElasticGraphStepStrategy extends AbstractTraversalStrategy {
    private static final ElasticGraphStepStrategy INSTANCE = new ElasticGraphStepStrategy();

    public static ElasticGraphStepStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.COMPUTER)) return;

        final Step<?, ?> startStep = TraversalHelper.getStart(traversal);
        if (startStep instanceof GraphStep) {
            final GraphStep<?> originalGraphStep = (GraphStep) startStep;
            ElasticGraph graph = originalGraphStep.getGraph(ElasticGraph.class);
            final ElasticGraphStep<?> elasticGraphStep = new ElasticGraphStep<>(null,originalGraphStep.getTraversal(), graph, originalGraphStep.getReturnClass(), originalGraphStep.getLabel(), originalGraphStep.getIds());
            TraversalHelper.replaceStep(startStep, (Step) elasticGraphStep, traversal);
            ElasticGraphStep lastElasticGraphStep = elasticGraphStep;
            Step<?, ?> currentStep = elasticGraphStep.getNextStep();
            while (true) {
                if (currentStep instanceof HasContainerHolder) {

                    lastElasticGraphStep.hasContainers.addAll(((HasContainerHolder) currentStep).getHasContainers());
                    if (currentStep.getLabel().isPresent()) {
                        final IdentityStep identityStep = new IdentityStep<>(traversal);
                        identityStep.setLabel(currentStep.getLabel().get());
                        TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
                    }
                    traversal.removeStep(currentStep);

                } else if (currentStep instanceof VertexStep) {

                    VertexStep<?> originalVertexStep = (VertexStep) currentStep;
                     ElasticGraphStep<?> graphStep = new ElasticGraphStep<>(originalVertexStep.getDirection(),originalVertexStep.getTraversal(), graph, originalVertexStep.getReturnClass(), originalVertexStep.getLabel(),new Object[0]);
                    lastElasticGraphStep = graphStep;
                    TraversalHelper.replaceStep(currentStep, (Step) graphStep, traversal);

                }
                else if (currentStep instanceof EdgeVertexStep){

                    EdgeVertexStep originalEdgeStep = (EdgeVertexStep) currentStep;
                    ElasticGraphStep<?> graphStep = new ElasticGraphStep<>(originalEdgeStep.getDirection(),originalEdgeStep.getTraversal(), graph, ElasticVertex.class, originalEdgeStep.getLabel(),new Object[0]);
                    lastElasticGraphStep = graphStep;
                    TraversalHelper.replaceStep(currentStep, (Step) graphStep, traversal);
                }

                else if (currentStep instanceof EmptyStep) {
                    break;
                } else {
                    //do nothing
                }

                currentStep = currentStep.getNextStep();
            }
            int i=1;
        }
    }
}
