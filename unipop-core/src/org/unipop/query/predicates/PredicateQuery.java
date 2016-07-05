package org.unipop.query.predicates;

import org.unipop.query.StepDescriptor;
import org.unipop.query.UniQuery;

public class PredicateQuery extends UniQuery {
    private final PredicatesHolder predicates;

    public PredicateQuery(PredicatesHolder predicates, StepDescriptor stepDescriptor) {
        super(stepDescriptor);
        this.predicates = predicates;
    }

    public PredicatesHolder getPredicates(){
        return predicates;
    }

    @Override
    public String toString() {
        return "PredicateQuery{" +
                "predicates=" + predicates +
                '}';
    }
}
