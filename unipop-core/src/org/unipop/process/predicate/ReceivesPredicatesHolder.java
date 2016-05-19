package org.unipop.process.predicate;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.unipop.query.predicates.PredicatesHolder;

public interface ReceivesPredicatesHolder<S, E> extends Step<S, E> {
    void addPredicate(PredicatesHolder predicatesHolder);
    PredicatesHolder getPredicates();
    void setLimit(int limit);
}
