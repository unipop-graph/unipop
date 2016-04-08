package org.unipop.query;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.unipop.query.UniQuery;

import java.util.List;

public class PredicateQuery extends UniQuery{
    private final List<HasContainer> predicates;

    public PredicateQuery(List<HasContainer> predicates, StepDescriptor stepDescriptor) {
        super(stepDescriptor);
        this.predicates = predicates;
    }

    public List<HasContainer> getPredicates(){
        return predicates;
    }
}
