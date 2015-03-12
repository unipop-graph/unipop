package com.tinkerpop.gremlin.elastic.process.graph.traversal.strategy;

import com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect.EdgeSearchStep;
import com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect.ElasticGraphStep;
import com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect.ElasticSearchStep;
import com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect.VertexSearchStep;
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
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;

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
            final ElasticGraphStep<?> elasticGraphStep = new ElasticGraphStep<>(originalGraphStep.getTraversal(), graph, originalGraphStep.getReturnClass(), originalGraphStep.getLabel(), originalGraphStep.getIds());
            TraversalHelper.replaceStep(startStep, (Step) elasticGraphStep, traversal);
            ElasticSearchStep lastElasticSearchStep = elasticGraphStep;
            Step<?, ?> currentStep = elasticGraphStep.getNextStep();
            while (true) {
                if (currentStep instanceof HasContainerHolder) {

                    lastElasticSearchStep.addPredicates(((HasContainerHolder) currentStep).getHasContainers());
                    if (currentStep.getLabel().isPresent()) {
                        final IdentityStep identityStep = new IdentityStep<>(traversal);
                        identityStep.setLabel(currentStep.getLabel().get());
                        TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
                    }
                    traversal.removeStep(currentStep);

                } else if (currentStep instanceof VertexStep) {

                    VertexStep<?> originalVertexStep = (VertexStep) currentStep;
                    Class<? extends Element> returnClassOfVertexStep = originalVertexStep.getReturnClass();
                    ElasticSearchStep newSearchStep;
                    if(Vertex.class.isAssignableFrom(returnClassOfVertexStep)){
                        newSearchStep = new VertexSearchStep<>(originalVertexStep.getTraversal(), originalVertexStep.getDirection(), graph.elasticService,returnClassOfVertexStep, originalVertexStep.getLabel(),originalVertexStep.getEdgeLabels());
                    }
                    else {
                        newSearchStep = new EdgeSearchStep(originalVertexStep.getTraversal(),originalVertexStep.getDirection(),graph.elasticService,originalVertexStep.getLabel(),originalVertexStep.getEdgeLabels());
                    }
                    TraversalHelper.replaceStep(currentStep, (Step) newSearchStep, traversal);
                    lastElasticSearchStep = newSearchStep;

                }
                else if (currentStep instanceof EdgeVertexStep){

                    EdgeVertexStep originalEdgeStep = (EdgeVertexStep) currentStep;
                    ElasticSearchStep newSearchStep = new VertexSearchStep<>(originalEdgeStep.getTraversal(),originalEdgeStep.getDirection(),graph.elasticService, Edge.class,originalEdgeStep.getLabel());
                    TraversalHelper.replaceStep(currentStep, (Step) newSearchStep, traversal);
                    lastElasticSearchStep = newSearchStep;
                }
                //empty step is the last step
                else if (currentStep instanceof EmptyStep) {
                    break;
                } else {
                    //continue on other Steps.
                }

                currentStep = currentStep.getNextStep();
            }
        }
    }
}
