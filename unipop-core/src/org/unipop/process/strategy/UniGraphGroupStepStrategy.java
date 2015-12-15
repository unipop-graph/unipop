package org.unipop.process.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.controller.aggregation.SemanticKeyTraversal;
import org.unipop.controller.aggregation.SemanticReducerTraversal;
import org.unipop.controller.aggregation.SemanticValuesTraversal;
import org.unipop.process.UniGraphGroupStep;
import org.unipop.process.UniGraphStartStep;
import org.unipop.process.UniGraphVertexStep;
import org.unipop.structure.UniGraph;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Created by Gilad on 03/11/2015.
 */
public class UniGraphGroupStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    //region AbstractTraversalStrategy Implementation
    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        Set<Class<? extends TraversalStrategy.ProviderOptimizationStrategy>> priorStrategies = new HashSet<>();
        priorStrategies.add(UniGraphPredicatesStrategy.class);
        priorStrategies.add(UniGraphGroupStepCountStrategy.class);
        return priorStrategies;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if(traversal.getEngine().isComputer()) return;

        Graph graph = traversal.getGraph().get();
        if(!(graph instanceof UniGraph)) return;

        UniGraph elasticGraph = (UniGraph) graph;

        TraversalHelper.getStepsOfAssignableClassRecursively(GroupStep.class, traversal).forEach(step -> {
            GroupStep groupStep = (GroupStep) step;

            UniGraphGroupStep elasticGroupStep = null;
            if (UniGraphVertexStep.class.isAssignableFrom(step.getPreviousStep().getClass()) &&
                    Edge.class.isAssignableFrom(((UniGraphVertexStep) step.getPreviousStep()).getReturnClass())) {
                UniGraphVertexStep elasticVertexStep = (UniGraphVertexStep) step.getPreviousStep();
                elasticGroupStep = new UniGraphGroupStep(
                        traversal,
                        elasticVertexStep.getReturnClass(),
                        elasticVertexStep.getPredicates(),
                        new Object[0],
                        elasticVertexStep.getEdgeLabels(),
                        Optional.of(elasticVertexStep.getDirection()), elasticGraph.getControllerManager());

            } else if (UniGraphStartStep.class.isAssignableFrom(step.getPreviousStep().getClass())) {
                UniGraphStartStep elasticGraphStep = (UniGraphStartStep) step.getPreviousStep();
                elasticGroupStep = new UniGraphGroupStep(
                        traversal,
                        elasticGraphStep.getReturnClass(),
                        elasticGraphStep.getPredicates(),
                        elasticGraphStep.getIds(),
                        new String[0],
                        Optional.empty(), elasticGraph.getControllerManager());
            }

            if (elasticGroupStep != null) {
                List<Traversal> groupTraversals = groupStep.getLocalChildren();

                //add conditions here
                Traversal keyTraversal = groupTraversals.size() > 0 ? groupTraversals.get(0) : null;
                Traversal valuesTraversal = groupTraversals.size() > 1 ? groupTraversals.get(1) : null;
                Traversal reducerTraversal = groupTraversals.size() > 2 ? groupTraversals.get(2) : null;

                SemanticKeyTraversal semanticKeyTraversal = translateKeyTraversal(keyTraversal);
                SemanticValuesTraversal semanticValuesTraversal = translateValuesTraversal(valuesTraversal);
                SemanticReducerTraversal semanticReducerTraversal = translateReducerTraversal(reducerTraversal, semanticValuesTraversal);

                boolean semanticKeyTranslationSuccess = keyTraversal != null && semanticKeyTraversal != null;
                boolean semanticValuesTranslationSuccess = valuesTraversal != null && semanticValuesTraversal != null;
                boolean semanticReducerTranslationSuccess = reducerTraversal != null && semanticReducerTraversal != null;

                if (semanticKeyTranslationSuccess && semanticValuesTranslationSuccess && semanticReducerTranslationSuccess) {
                    elasticGroupStep.setKeyTraversal(semanticKeyTraversal);
                    elasticGroupStep.setValuesTraversal(semanticValuesTraversal);
                    elasticGroupStep.setReducerTraversal(semanticReducerTraversal);

                    TraversalHelper.replaceStep(step.getPreviousStep(), elasticGroupStep, traversal);
                    traversal.removeStep(step);

                    insertStartStepWhenTraversalIsInternal(traversal, elasticGroupStep);
                }
            }
        });
    }


    private SemanticKeyTraversal translateKeyTraversal(Traversal keyTraversal) {
        if (ElementValueTraversal.class.isAssignableFrom(keyTraversal.getClass())) {
            ElementValueTraversal elementValueTraversal = (ElementValueTraversal)keyTraversal;
            return new SemanticKeyTraversal(SemanticKeyTraversal.Type.property, elementValueTraversal.getPropertyKey());
        }

        if (TokenTraversal.class.isAssignableFrom(keyTraversal.getClass())) {
            TokenTraversal tokenTraversal = (TokenTraversal)keyTraversal;
            switch (tokenTraversal.getToken()) {
                case id:
                    //not supported for elastic but cold be supported for other stores.
                    return null;

                case label:
                    return new SemanticKeyTraversal(SemanticKeyTraversal.Type.property, T.label.getAccessor());
            }
        }

        if (Traversal.Admin.class.isAssignableFrom(keyTraversal.getClass())) {
            Traversal.Admin adminTraversal = (Traversal.Admin)keyTraversal;
            if (adminTraversal.getSteps().size() != 1 ||
                    !isTraversalStepAssignableFrom(adminTraversal, 0, PropertiesStep.class)) {
                return null;
            }

            PropertiesStep propertiesStep = (PropertiesStep)adminTraversal.getSteps().get(0);
            if (propertiesStep.getReturnType() != PropertyType.VALUE ||
                    propertiesStep.getPropertyKeys() == null ||
                    propertiesStep.getPropertyKeys().length != 1) {
                return null;
            }

            return new SemanticKeyTraversal(SemanticKeyTraversal.Type.property, propertiesStep.getPropertyKeys()[0]);
        }

        return null;
    }

    private SemanticValuesTraversal translateValuesTraversal(Traversal valuesTraversal) {
        if (valuesTraversal == null) {
            return null;
        }

        if (ElementValueTraversal.class.isAssignableFrom(valuesTraversal.getClass())) {
            ElementValueTraversal elementValueTraversal = (ElementValueTraversal)valuesTraversal;
            return new SemanticValuesTraversal(SemanticValuesTraversal.Type.property, elementValueTraversal.getPropertyKey());
        } else if (TokenTraversal.class.isAssignableFrom(valuesTraversal.getClass())) {
            TokenTraversal tokenTraversal = (TokenTraversal)valuesTraversal;
            switch (tokenTraversal.getToken()) {
                case id:
                    //not supported for elastic but cold be supported for other stores.
                    return null;

                case label:
                    return new SemanticValuesTraversal(SemanticValuesTraversal.Type.property, T.label.getAccessor());
            }
        }

        if (Traversal.Admin.class.isAssignableFrom(valuesTraversal.getClass())) {
            Traversal.Admin adminTraversal = (Traversal.Admin)valuesTraversal;
            if (adminTraversal.getSteps().size() != 1 ||
                    !isTraversalStepAssignableFrom(adminTraversal, 0, PropertiesStep.class)) {
                return null;
            }

            PropertiesStep propertiesStep = (PropertiesStep)adminTraversal.getSteps().get(0);
            if (propertiesStep.getReturnType() != PropertyType.VALUE ||
                    propertiesStep.getPropertyKeys() == null ||
                    propertiesStep.getPropertyKeys().length != 1) {
                return null;
            }

            return new SemanticValuesTraversal(SemanticValuesTraversal.Type.property, propertiesStep.getPropertyKeys()[0]);
        }

        return null;
    }

    private SemanticReducerTraversal translateReducerTraversal(Traversal reducerTraversal, SemanticValuesTraversal semanticValuesTraversal) {
        if (semanticValuesTraversal == null || reducerTraversal == null) {
            return null;
        }

        if (Traversal.Admin.class.isAssignableFrom(reducerTraversal.getClass())) {
            Traversal.Admin adminTraversal = (Traversal.Admin)reducerTraversal;
            if (adminTraversal.getSteps().size() == 1 &&
                    isTraversalStepAssignableFrom(adminTraversal, 0, CountLocalStep.class)) {
                return new SemanticReducerTraversal(SemanticReducerTraversal.Type.count, semanticValuesTraversal.getKey());
            }

            if (adminTraversal.getSteps().size() == 1 &&
                    isTraversalStepAssignableFrom(adminTraversal, 0, MaxLocalStep.class)) {
                return new SemanticReducerTraversal(SemanticReducerTraversal.Type.max, semanticValuesTraversal.getKey());
            }

            if (adminTraversal.getSteps().size() == 1 &&
                    isTraversalStepAssignableFrom(adminTraversal, 0, MinLocalStep.class)) {
                return new SemanticReducerTraversal(SemanticReducerTraversal.Type.min, semanticValuesTraversal.getKey());
            }

            if (adminTraversal.getSteps().size() == 2 &&
                    isTraversalStepAssignableFrom(adminTraversal, 0, DedupLocalStep.class) &&
                    isTraversalStepAssignableFrom(adminTraversal, 1, CountLocalStep.class)) {
                return new SemanticReducerTraversal(SemanticReducerTraversal.Type.cardinality, semanticValuesTraversal.getKey());
            }
        }

        return null;
    }

    private boolean isTraversalStepAssignableFrom(Traversal.Admin adminTraversal, int stepNumber, Class stepClass) {
        return stepClass.isAssignableFrom(adminTraversal.getSteps().get(stepNumber).getClass());
    }

    //endregion

    //region Private Methods
    private void insertStartStepWhenTraversalIsInternal(final Traversal.Admin<?, ?> traversal, Step step) {
        if (!traversal.getParent().equals(EmptyStep.instance())) {
            StartStep startStep = new StartStep(traversal);
            TraversalHelper.insertBeforeStep(startStep, step, traversal);
        }
    }
    //endregion
}
