package org.unipop.process.properties;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.unipop.process.repeat.UniGraphRepeatStep;
import org.unipop.process.repeat.UniGraphRepeatStepStrategy;
import org.unipop.process.edge.EdgeStepsStrategy;
import org.unipop.process.edge.UniGraphEdgeOtherVertexStep;
import org.unipop.process.edge.UniGraphEdgeVertexStep;
import org.unipop.process.start.UniGraphStartStepStrategy;
import org.unipop.process.vertex.UniGraphVertexStepStrategy;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by sbarzilay on 6/8/16.
 */
public class UniGraphPropertiesStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        return Sets.newHashSet(UniGraphStartStepStrategy.class, UniGraphVertexStepStrategy.class, UniGraphRepeatStepStrategy.class, EdgeStepsStrategy.class);
    }

    private void handlePropertiesSteps(String[] propertyKeys, PropertyFetcher propertyFetcher) {
        if (propertyFetcher != null) {
            if (propertyKeys.length > 0)
                for (String key : propertyKeys) {
                    propertyFetcher.addPropertyKey(key);
                }
            else
                propertyFetcher.fetchAllKeys();
        }
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfClass(OrderGlobalStep.class, traversal).forEach(orderGlobalStep -> {
            Collection<PropertyFetcher> propertyFetchers = getPropertyFetcherStepOf(orderGlobalStep, traversal);
            if (propertyFetchers != null) {
                propertyFetchers.forEach(propertyFetcher -> {
                    if ((traversal.getParent() instanceof ConnectiveStep) || TraversalHelper.hasStepOfClass(MatchStep.MatchStartStep.class, traversal)) {
                        propertyFetcher.fetchAllKeys();
                    } else orderGlobalStep.getLocalChildren().forEach(t -> {
                        if (t instanceof ElementValueTraversal) {
                            String propertyKey = ((ElementValueTraversal) t).getPropertyKey();
                            handlePropertiesSteps(new String[]{propertyKey}, propertyFetcher);
                        }
                    });
                });
            }
        });

        TraversalHelper.getStepsOfClass(DedupGlobalStep.class, traversal).forEach(dedupGlobalStep -> {
            Collection<PropertyFetcher> propertyFetchers = getPropertyFetcherStepOf(dedupGlobalStep, traversal);
            if (propertyFetchers != null) {
                propertyFetchers.forEach(propertyFetcher -> {
                    if ((traversal.getParent() instanceof ConnectiveStep) || TraversalHelper.hasStepOfClass(MatchStep.MatchStartStep.class, traversal)) {
                        propertyFetcher.fetchAllKeys();
                    } else if (dedupGlobalStep.getLocalChildren().size() > 0)
                        if (dedupGlobalStep.getLocalChildren().get(0) instanceof ElementValueTraversal) {
                            String propertyKey = ((ElementValueTraversal) dedupGlobalStep.getLocalChildren().get(0)).getPropertyKey();
                            handlePropertiesSteps(new String[]{propertyKey}, propertyFetcher);
                        }
                });
            }
        });

        TraversalHelper.getStepsOfClass(PropertyMapStep.class, traversal).forEach(propertyMapStep -> {
            Collection<PropertyFetcher> propertyFetchers = getPropertyFetcherStepOf(propertyMapStep, traversal);
            if (propertyFetchers != null) {
                propertyFetchers.forEach(propertyFetcher -> {
                    if ((traversal.getParent() instanceof ConnectiveStep) || TraversalHelper.hasStepOfClass(MatchStep.MatchStartStep.class, traversal)) {
                        propertyFetcher.fetchAllKeys();
                    } else
                        handlePropertiesSteps(propertyMapStep.getPropertyKeys(), propertyFetcher);
                });
            }
        });

        TraversalHelper.getStepsOfClass(PropertiesStep.class, traversal).forEach(propertiesStep -> {
            Collection<PropertyFetcher> propertyFetchers = getPropertyFetcherStepOf(propertiesStep, traversal);
            if (propertyFetchers != null) {
                propertyFetchers.forEach(propertyFetcher -> {
                    if ((traversal.getParent() instanceof ConnectiveStep) || TraversalHelper.hasStepOfClass(MatchStep.MatchStartStep.class, traversal)) {
                        propertyFetcher.fetchAllKeys();
                    } else
                        handlePropertiesSteps(propertiesStep.getPropertyKeys(), propertyFetcher);
                });
            }
        });

        TraversalHelper.getStepsOfClass(HasStep.class, traversal).forEach(hasStep -> {
            Collection<PropertyFetcher> propertyFetchers = getPropertyFetcherStepOf(hasStep, traversal);
            if (propertyFetchers != null) {
                propertyFetchers.forEach(propertyFetcher -> {
                    if ((traversal.getParent() instanceof ConnectiveStep) || TraversalHelper.hasStepOfClass(MatchStep.MatchStartStep.class, traversal)) {
                        propertyFetcher.fetchAllKeys();
                    } else {
                        List<HasContainer> hasContainers = hasStep.getHasContainers();
                        hasContainers.stream().map(HasContainer::getKey).forEach(propertyFetcher::addPropertyKey);
                    }
                });
            }
        });

        TraversalHelper.getStepsOfClass(WherePredicateStep.class, traversal).forEach(wherePredicateStep -> {
            Collection<PropertyFetcher> propertyFetchers = getPropertyFetcherStepOf(wherePredicateStep, traversal);
            if (propertyFetchers != null)
                propertyFetchers.forEach(PropertyFetcher::fetchAllKeys);
        });

        TraversalHelper.getStepsOfAssignableClass(FilterStep.class, traversal).forEach(filterStep -> {
            Collection<PropertyFetcher> propertyFetchers = getPropertyFetcherStepOf(filterStep, traversal);
            if (!(filterStep instanceof HasStep)
                    && !(filterStep instanceof WherePredicateStep)
                    && !(filterStep instanceof DedupGlobalStep)
                    && !(filterStep instanceof RangeGlobalStep)) {
                if (propertyFetchers != null)
                    propertyFetchers.forEach(PropertyFetcher::fetchAllKeys);
            }
        });

        TraversalHelper.getStepsOfClass(PathStep.class, traversal).forEach(pathStep -> {
            List<PropertyFetcher> propertyFetchers = getAllPropertyFetchersOf(pathStep, traversal);
            pathStep.getLocalChildren().forEach(t -> {
                if (t instanceof ElementValueTraversal) {
                    String propertyKey = ((ElementValueTraversal) t).getPropertyKey();
                    propertyFetchers.forEach(propertyFetcher -> handlePropertiesSteps(new String[]{propertyKey}, propertyFetcher));
                }
            });
        });

        TraversalHelper.getStepsOfClass(TreeStep.class, traversal).forEach(treeStep -> {
            List<PropertyFetcher> propertyFetchers = getAllPropertyFetchersOf(treeStep, traversal);
            treeStep.getLocalChildren().forEach(t -> {
                if (t instanceof ElementValueTraversal) {
                    String propertyKey = ((ElementValueTraversal) t).getPropertyKey();
                    propertyFetchers.forEach(propertyFetcher -> handlePropertiesSteps(new String[]{propertyKey}, propertyFetcher));
                }
            });
        });

        TraversalHelper.getStepsOfClass(TreeSideEffectStep.class, traversal).forEach(treeStep -> {
            List<PropertyFetcher> propertyFetchers = getAllPropertyFetchersOf(treeStep, traversal);
            treeStep.getLocalChildren().forEach(t -> {
                if (t instanceof ElementValueTraversal) {
                    String propertyKey = ((ElementValueTraversal) t).getPropertyKey();
                    propertyFetchers.forEach(propertyFetcher -> handlePropertiesSteps(new String[]{propertyKey}, propertyFetcher));
                }
            });
        });

        TraversalHelper.getStepsOfAssignableClass(MapStep.class, traversal).forEach(mapStep -> {
            Collection<PropertyFetcher> propertyFetchers = getPropertyFetcherStepOf(mapStep, traversal);
            if (!(mapStep instanceof PropertyMapStep) && !(mapStep instanceof SelectOneStep) && !(mapStep instanceof PathStep)) {
                if (propertyFetchers != null)
                    propertyFetchers.forEach(PropertyFetcher::fetchAllKeys);
            }
        });

        TraversalHelper.getStepsOfAssignableClass(GroupStep.class, traversal).forEach(groupStep -> {
            Collection<PropertyFetcher> propertyFetchers = getPropertyFetcherStepOf(groupStep, traversal);
            groupStep.getLocalChildren().forEach(t -> {
                if (propertyFetchers != null)
                propertyFetchers.forEach(propertyFetcher -> {
                    if (t instanceof ElementValueTraversal) {
                        String propertyKey = ((ElementValueTraversal) t).getPropertyKey();
                        handlePropertiesSteps(new String[]{propertyKey}, propertyFetcher);
                    } else if (t instanceof DefaultGraphTraversal) {
                        List<Step> steps = ((DefaultGraphTraversal) t).getSteps();
                        steps.forEach(step -> {
                            if (step instanceof TraversalMapStep) {
                                ((TraversalMapStep) step).getLocalChildren().forEach(t2 -> {
                                    if (t2 instanceof ElementValueTraversal) {
                                        String propertyKey = ((ElementValueTraversal) t2).getPropertyKey();
                                        handlePropertiesSteps(new String[]{propertyKey}, propertyFetcher);
                                    }
                                });
                            } else {
                                if (step instanceof PropertiesStep) {
                                    String[] propertyKeys = ((PropertiesStep) step).getPropertyKeys();
                                    if (propertyKeys.length == 0)
                                        propertyFetcher.fetchAllKeys();
                                    else
                                        for (String propertyKey : propertyKeys) {
                                            propertyFetcher.addPropertyKey(propertyKey);
                                        }
                                }
                            }
                        });
                    }
                });
            });
        });

        TraversalHelper.getStepsOfAssignableClass(GroupCountStep.class, traversal).forEach(groupCountStep -> {
            Collection<PropertyFetcher> propertyFetchers = getPropertyFetcherStepOf(groupCountStep, traversal);
            groupCountStep.getLocalChildren().forEach(t -> {
                if (t instanceof ElementValueTraversal) {
                    String propertyKey = ((ElementValueTraversal) t).getPropertyKey();
                    propertyFetchers.forEach(propertyFetcher -> handlePropertiesSteps(new String[]{propertyKey}, propertyFetcher));
                }
            });
        });

        TraversalHelper.getStepsOfAssignableClass(ReducingBarrierStep.class, traversal).forEach(reducingBarrierStep -> {
            Collection<PropertyFetcher> propertyFetchers = getPropertyFetcherStepOf(reducingBarrierStep, traversal);
            if (!(reducingBarrierStep instanceof FoldStep)
                    && !(reducingBarrierStep instanceof GroupStep)
                    && !(reducingBarrierStep instanceof GroupCountStep)
                    && !(reducingBarrierStep instanceof TreeStep)) {
                if (propertyFetchers != null)
                    propertyFetchers.forEach(PropertyFetcher::fetchAllKeys);
            }
        });

        TraversalHelper.getStepsOfAssignableClass(SideEffectStep.class, traversal).forEach(sideEffectStep -> {
            Collection<PropertyFetcher> propertyFetchers = getPropertyFetcherStepOf(sideEffectStep, traversal);
            if (propertyFetchers != null)
                propertyFetchers.forEach(PropertyFetcher::fetchAllKeys);
        });

        TraversalHelper.getStepsOfAssignableClass(SelectOneStep.class, traversal).forEach(selectOneStep -> {
            List<PropertyFetcher> allPropertyFetchersOf = getAllPropertyFetchersOf(selectOneStep, traversal);
            allPropertyFetchersOf.forEach(propertyFetcher -> {
                Set<String> scopeKeys = selectOneStep.getScopeKeys();
                Set<String> labels = ((Step) propertyFetcher).getLabels();
                Optional<String> first = labels.stream().filter(scopeKeys::contains).findFirst();
                // TODO: fetch only relevant properties
                if (first.isPresent()) {
                    propertyFetcher.fetchAllKeys();
                }
            });
        });

        TraversalHelper.getStepsOfAssignableClass(SelectStep.class, traversal).forEach(selectStep -> {
            List<PropertyFetcher> allPropertyFetchersOf;
            if (traversal.getParent().asStep() instanceof LocalStep) {
                Step<?, ?> localStep = traversal.getParent().asStep();
                allPropertyFetchersOf = getAllPropertyFetchersOf(localStep, traversal);
            } else {
                allPropertyFetchersOf = getAllPropertyFetchersOf(selectStep, traversal);
            }
            allPropertyFetchersOf.forEach(propertyFetcher -> {
                Set<String> scopeKeys = selectStep.getScopeKeys();
                Set<String> labels = ((Step) propertyFetcher).getLabels();
                Optional<String> first = labels.stream().filter(scopeKeys::contains).findFirst();
                // TODO: fetch only relevant properties
                if (first.isPresent()) {
                    propertyFetcher.fetchAllKeys();
                }
            });
        });


        TraversalHelper.getStepsOfAssignableClass(LambdaMapStep.class, traversal).forEach(lambdaMapStep -> {
            List<PropertyFetcher> allPropertyFetchersOf = getAllPropertyFetchersOf(lambdaMapStep, traversal);
            if (allPropertyFetchersOf.size() > 0)
                allPropertyFetchersOf.forEach(PropertyFetcher::fetchAllKeys);
        });

        TraversalHelper.getStepsOfClass(OrderGlobalStep.class, traversal).forEach(orderGlobalStep -> {
            orderGlobalStep.getLocalChildren().forEach(child -> TraversalHelper.getStepsOfAssignableClass(LambdaMapStep.class, (Traversal.Admin) child).forEach(lambdaMapStep -> {
                Collection<PropertyFetcher> propertyFetcherStepsOf = getPropertyFetcherStepOf(orderGlobalStep, traversal);
                if (propertyFetcherStepsOf != null)
                    propertyFetcherStepsOf.forEach(PropertyFetcher::fetchAllKeys);
            }));
        });

        TraversalHelper.getStepsOfAssignableClass(UniGraphRepeatStep.class, traversal).forEach(uniGraphRepeatStepTemp -> {
            Traversal.Admin repeatTraversal = uniGraphRepeatStepTemp.getRepeatTraversal();
            Optional<PropertyFetcher> lastStepOfAssignableClass = TraversalHelper.getLastStepOfAssignableClass(PropertyFetcher.class, repeatTraversal);
            if (lastStepOfAssignableClass.isPresent()) {
                lastStepOfAssignableClass.get().fetchAllKeys();
            }
        });

        TraversalHelper.getStepsOfAssignableClass(AddEdgeStep.class, traversal).forEach(addEdgeStep -> {
            List<PropertyFetcher> allPropertyFetchersOf = getAllPropertyFetchersOf(addEdgeStep, traversal);
            if (allPropertyFetchersOf.size() > 0)
                allPropertyFetchersOf.forEach(PropertyFetcher::fetchAllKeys);
        });

        Optional<PropertyFetcher> lastStepOfAssignableClass = TraversalHelper.getLastStepOfAssignableClass(PropertyFetcher.class, traversal);
        if (lastStepOfAssignableClass.isPresent()) {
            lastStepOfAssignableClass.get().fetchAllKeys();
        }

        TraversalHelper.getStepsOfAssignableClass(UniGraphEdgeOtherVertexStep.class, traversal).forEach(uniGraphEdgeOtherVertexStep -> {
            Collection<PropertyFetcher> propertyFetcherStepsOf = getPropertyFetcherStepOf(uniGraphEdgeOtherVertexStep, traversal);
            if (propertyFetcherStepsOf != null) {
                Set<String> keys = uniGraphEdgeOtherVertexStep.getKeys();
                propertyFetcherStepsOf.forEach(propertyFetcher -> {
                    if (keys != null)
                        keys.forEach(propertyFetcher::addPropertyKey);
                    else
                        propertyFetcher.fetchAllKeys();
                });
            }
        });

        TraversalHelper.getStepsOfAssignableClass(UniGraphEdgeVertexStep.class, traversal).forEach(uniGraphEdgeVertexStep -> {
            Collection<PropertyFetcher> propertyFetcherStepsOf = getPropertyFetcherStepOf(uniGraphEdgeVertexStep, traversal);
            if (propertyFetcherStepsOf != null) {
                Set<String> keys = uniGraphEdgeVertexStep.getKeys();
                propertyFetcherStepsOf.forEach(propertyFetcher -> {
                    if (keys != null)
                        keys.forEach(propertyFetcher::addPropertyKey);
                    else
                        propertyFetcher.fetchAllKeys();
                });
            }
        });
    }


    private List<PropertyFetcher> getAllPropertyFetchersOf(Step step, Traversal.Admin<?, ?> traversal) {
        List<PropertyFetcher> propertyFetchers = new ArrayList<>();
        Step previous = step.getPreviousStep();
        while (!(previous instanceof EmptyStep)) {
            if (previous instanceof PropertyFetcher)
                propertyFetchers.add((PropertyFetcher) previous);
            if (previous instanceof TraversalParent) {
                ((TraversalParent) previous).getLocalChildren()
                        .forEach(t -> t.getSteps().stream()
                                .filter(s -> s instanceof PropertyFetcher)
                                .map(p -> ((PropertyFetcher) p)).forEach(propertyFetchers::add));
                ((TraversalParent) previous).getGlobalChildren()
                        .forEach(t -> t.getSteps().stream()
                                .filter(s -> s instanceof PropertyFetcher)
                                .map(p -> ((PropertyFetcher) p)).forEach(propertyFetchers::add));
            }
            previous = previous.getPreviousStep();
        }
        return propertyFetchers;
    }

    private Collection<PropertyFetcher> getPropertyFetcherStepOf(Step step, Traversal.Admin<?, ?> traversal) {
        Step previous = step.getPreviousStep();
        while (!(previous instanceof PropertyFetcher)) {
            if (previous instanceof DedupGlobalStep || previous instanceof OrderGlobalStep)
                previous = previous.getPreviousStep();
            else if (previous instanceof EmptyStep) {
                TraversalParent parent = traversal.getParent();
                List<PropertyFetcher> propertyFetchers = parent.getLocalChildren().stream().flatMap(child -> TraversalHelper.getStepsOfAssignableClassRecursively(PropertyFetcher.class, child).stream()).collect(Collectors.toList());
                if (propertyFetchers.size() > 0)
                    previous = (Step) propertyFetchers.get(propertyFetchers.size() - 1);
                else
                    return null;
            } else if (previous instanceof TraversalParent) {
                List<PropertyFetcher> propertyFetchers = Stream.concat(
                        ((TraversalParent) previous).getLocalChildren().stream(),
                        ((TraversalParent) previous).getGlobalChildren().stream())
                        .flatMap(child ->
                                TraversalHelper.getStepsOfAssignableClassRecursively(PropertyFetcher.class, child)
                                        .stream()).collect(Collectors.toList());
                if (propertyFetchers.size() > 0)
                    return propertyFetchers;
//                    previous = (Step) propertyFetchers.get(propertyFetchers.size() - 1);
                else
                    return null;
            } else if (previous instanceof WhereTraversalStep.WhereStartStep)
                return null;
            else
                previous = previous.getPreviousStep();
        }
        return Collections.singleton((PropertyFetcher) previous);
    }
}
