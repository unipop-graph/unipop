package org.unipop.elastic2.controller.schema.helpers;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.controller.ExistsP;

import java.util.Arrays;
import java.util.stream.StreamSupport;

/**
 * Created by Gilad on 13/10/2015.
 */
public class HasContainersTranslator {
    public void updateSearchBuilder(SearchBuilder searchBuilder, Iterable<HasContainer> hasContainers) {
        for (HasContainer hasContainer : hasContainers) {
            applyHasContainer(searchBuilder, hasContainer);
        }
    }

    private void applyHasContainer(SearchBuilder searchBuilder, HasContainer hasContainer) {
        if (Graph.Hidden.isHidden(hasContainer.getKey())) {
            applyHiddenHasContainer(searchBuilder, hasContainer);
            return;
        }

        if (hasContainer.getPredicate() instanceof ExistsP) {
            searchBuilder.getQueryBuilder().seekRoot().query().filtered().filter().bool().must()
                    .exists(hasContainer.getKey());
        }

        if (hasContainer.getBiPredicate() != null ) {
            if (hasContainer.getBiPredicate() instanceof Compare) {
                Compare compare = (Compare) hasContainer.getBiPredicate();
                switch (compare) {
                    case eq:
                        searchBuilder.getQueryBuilder().seekRoot().query().filtered().filter().bool().must()
                                .term(hasContainer.getKey(), hasContainer.getValue());
                        break;
                    case neq:
                        searchBuilder.getQueryBuilder().seekRoot().query().filtered().filter().bool().mustNot()
                                .term(hasContainer.getKey(), hasContainer.getValue());
                        break;
                    case gt:
                    case gte:
                    case lt:
                    case lte:
                        searchBuilder.getQueryBuilder().seekRoot().query().filtered().filter().bool().must()
                                .range(hasContainer.getKey(), compare, hasContainer.getValue());
                        break;
                    default:
                        throw new IllegalArgumentException(String.format("predicate not supported in has step: %s" + hasContainer.toString()));
                }
            } else if (hasContainer.getBiPredicate() instanceof Contains) {
                Contains contains = (Contains) hasContainer.getBiPredicate();
                switch (contains) {
                    case within:
                        if (hasContainer.getValue() != null) {
                            searchBuilder.getQueryBuilder().seekRoot().query().filtered().filter().bool().must()
                                    .terms(hasContainer.getKey(), hasContainer.getValue());
                        } else {
                            searchBuilder.getQueryBuilder().seekRoot().query().filtered().filter().bool().must()
                                    .exists(hasContainer.getKey());
                        }
                        break;
                    case without:
                        if (hasContainer.getValue() != null) {
                            searchBuilder.getQueryBuilder().seekRoot().query().filtered().filter().bool().mustNot()
                                    .terms(hasContainer.getKey(), hasContainer.getValue());
                        } else {
                            searchBuilder.getQueryBuilder().seekRoot().query().filtered().filter().bool().mustNot()
                                    .exists(hasContainer.getKey());
                        }
                        break;

                    default:
                        throw new IllegalArgumentException(String.format("predicate not supported in has step: %s" + hasContainer.toString()));
                }
            }
        }
    }

    private void applyHiddenHasContainer(SearchBuilder searchBuilder, HasContainer hasContainer) {
        String plainKey = Graph.Hidden.unHide(hasContainer.getKey());
        switch (plainKey) {
            case "id":
                if (hasContainer.getBiPredicate() != null) {
                    if (hasContainer.getBiPredicate() instanceof Compare) {
                        Compare compare = (Compare) hasContainer.getBiPredicate();
                        switch (compare) {
                            case eq:
                                searchBuilder.getQueryBuilder().seekRoot().query().filtered().filter().bool().must()
                                        .ids(Arrays.asList(hasContainer.getValue()));
                                break;

                            case neq:
                                searchBuilder.getQueryBuilder().seekRoot().query().filtered().filter().bool().mustNot()
                                        .ids(Arrays.asList(hasContainer.getValue()));
                                break;

                            default:
                                throw new IllegalArgumentException(String.format("predicate not supported in has step: %s" + hasContainer.toString()));

                        }
                    } else if (hasContainer.getBiPredicate() instanceof Contains) {
                        Contains contains = (Contains) hasContainer.getBiPredicate();
                        switch (contains) {
                            case within:
                                if (hasContainer.getValue() != null) {
                                    searchBuilder.getQueryBuilder().seekRoot().query().filtered().filter().bool().must()
                                            .ids(Arrays.asList(convertValueToStringArray(hasContainer.getValue())),
                                                 searchBuilder.getTypes().stream().toArray(String[]::new));
                                }
                                break;

                            case without:
                                if (hasContainer.getValue() != null) {
                                    searchBuilder.getQueryBuilder().seekRoot().query().filtered().filter().bool().mustNot()
                                            .ids(Arrays.asList(convertValueToStringArray(hasContainer.getValue())),
                                                 searchBuilder.getTypes().stream().toArray(String[]::new));
                                }

                            default:
                                throw new IllegalArgumentException(String.format("predicate not supported in has step: %s" + hasContainer.toString()));
                        }
                    }
                }
                break;

            case "label":
                if (hasContainer.getBiPredicate() != null) {
                    if (hasContainer.getBiPredicate() instanceof Compare) {
                        Compare compare = (Compare) hasContainer.getBiPredicate();
                        switch (compare) {
                            case eq:
                                searchBuilder.getTypes().clear();
                                searchBuilder.getTypes().add(hasContainer.getValue().toString());
                                break;

                            case neq:
                                searchBuilder.getTypes().remove(hasContainer.getValue().toString());
                                break;

                            default:
                                throw new IllegalArgumentException(String.format("predicate not supported in has step: %s" + hasContainer.toString()));
                        }
                    } else if (hasContainer.getBiPredicate() instanceof Contains) {
                        Contains contains = (Contains) hasContainer.getBiPredicate();
                        switch (contains) {
                            case within:
                                if (hasContainer.getValue() != null) {
                                    searchBuilder.getTypes().clear();

                                    searchBuilder.getTypes().addAll(Arrays.asList(convertValueToStringArray(hasContainer.getValue())));
                                }
                                break;

                            case without:

                                searchBuilder.getTypes().removeAll(Arrays.asList(convertValueToStringArray(hasContainer.getValue())));
                                break;

                            default:
                                throw new IllegalArgumentException(String.format("predicate not supported in has step: %s" + hasContainer.toString()));
                        }
                    }
                }
                break;
        }
    }

    private String[] convertValueToStringArray(Object value) {
        if (value instanceof String[]) {
            return (String[]) value;
        } else if (Iterable.class.isAssignableFrom(value.getClass())) {
            return StreamSupport.stream(((Iterable<String>)value).spliterator(), false).toArray(String[]::new);
        }

        return null;
    }

}
