package org.unipop.query.predicates;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.query.StepDescriptor;
import org.unipop.query.UniQuery;

public class PredicateQuery<E extends Element> extends UniQuery {
    private final PredicatesHolder predicates;

    public PredicateQuery(PredicatesHolder predicates, StepDescriptor stepDescriptor) {
        super(stepDescriptor);
        this.predicates = predicates;
    }

    public PredicatesHolder getPredicates(){
        return predicates;
    }

    public boolean test(E element, PredicatesHolder predicates) {
        if(predicates.getClause().equals(PredicatesHolder.Clause.And)) {
            if (!HasContainer.testAll(element, predicates.getPredicates())) return false;

            for (PredicatesHolder child : predicates.getChildren()) {
                if (!test(element, child)) return false;
            }
            return true;
        }
        else {
            for(HasContainer has : predicates.getPredicates()) {
                if (has.test(element)) return true;
            }
            for (PredicatesHolder child : predicates.getChildren()) {
                if (test(element, child)) return true;
            }
            return false;
        }
    }

    @Override
    public String toString() {
        return "PredicateQuery{" +
                "predicates=" + predicates +
                '}';
    }
}
