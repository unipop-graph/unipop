package org.unipop.query.aggregation;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.query.StepDescriptor;
import org.unipop.query.controller.UniQueryController;
import org.unipop.query.PredicateQuery;

import java.util.List;

public class ReduceQuery extends PredicateQuery {

    public ReduceQuery(List<HasContainer> predicates, StepDescriptor stepDescriptor) {
        super(predicates, stepDescriptor);
    }

    public interface SearchController<E extends Element> extends UniQueryController {
        long count(ReduceQuery uniQuery);
    }
}
