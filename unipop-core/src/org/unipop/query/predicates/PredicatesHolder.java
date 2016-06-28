package org.unipop.query.predicates;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Stream;

public class PredicatesHolder {

    public enum Clause {
        And,
        Or,
        Abort
    }

    private Clause clause;
    private Set<HasContainer> predicates;
    private Set<PredicatesHolder> children;

    public PredicatesHolder(Clause clause, Set<HasContainer> predicates, Set<PredicatesHolder> children) {
        this.clause = clause;
        this.predicates = predicates != null ? predicates : Collections.emptySet();
        this.children = children != null ? children : Collections.emptySet();
    }

    public PredicatesHolder.Clause getClause() {
        return this.clause;
    }

    public Set<HasContainer> getPredicates() {
        return predicates;
    }

    public Set<PredicatesHolder> getChildren() {
        return children;
    }

    public boolean hasPredicates() {
        return getPredicates().size() > 0;
    }

    public boolean hasChildren() {
        return getChildren().size() > 0;
    }

    public boolean isEmpty() {
        return !hasPredicates() && !hasChildren();
    }

    public boolean notEmpty() {
        return !isEmpty();
    }

    public boolean isAborted() {
        return this.clause.equals(Clause.Abort);
    }

    public boolean notAborted() {
        return !isAborted();
    }

    public Stream<HasContainer> findKey(String key) {
        return predicates.stream().filter(has -> has.getKey().equals(key));
    }
}
