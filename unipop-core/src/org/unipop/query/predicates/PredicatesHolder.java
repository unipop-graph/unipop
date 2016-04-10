package org.unipop.query.predicates;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.ArrayList;
import java.util.Collection;

public class PredicatesHolder {
    public enum Clause{
        And,
        Or
    }

    private Collection<HasContainer> predicates = new ArrayList<>();
    private Clause clause;
    private Collection<PredicatesHolder> children = new ArrayList<>();

    public PredicatesHolder(Clause clause) {
        this.clause = clause;
    }

    public Collection<HasContainer> getPredicates() {
        return predicates;
    }

    public Clause getClause() {
        return clause;
    }

    public Collection<PredicatesHolder> getChildren() {
        return children;
    }

    public void add(HasContainer hasContainer) {
        predicates.add(hasContainer);
    }

    public void add(PredicatesHolder predicatesHolder) {
        children.add(predicatesHolder);
    }
}
