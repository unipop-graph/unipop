package org.unipop.query.search;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.StepDescriptor;
import org.unipop.query.controller.UniQueryController;

import java.util.Iterator;

public class SearchQuery<E extends Element> extends PredicateQuery {
    private final Class<E> returnType;
    private final int limit;

    public SearchQuery(Class<E> returnType, PredicatesHolder predicates, int limit, StepDescriptor stepDescriptor) {
        super(predicates, stepDescriptor);
        this.returnType = returnType;
        this.limit = limit;
    }

    public Class<E> getReturnType(){
        return returnType;
    }

    public int getLimit(){
        return limit;
    }

    public interface SearchController extends UniQueryController {
        <E extends Element> Iterator<E> search(SearchQuery<E> uniQuery);
    }

    @Override
    public String toString() {
        return "SearchQuery{" +
                "returnType=" + returnType +
                ", limit=" + limit +
                '}';
    }
}
