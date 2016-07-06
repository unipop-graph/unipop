package org.unipop.query.search;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.StepDescriptor;
import org.unipop.query.controller.UniQueryController;

import java.util.Iterator;
import java.util.Set;

public class SearchQuery<E extends Element> extends PredicateQuery {
    private final Class<E> returnType;
    private final int limit;
    private Set<String> propertyKeys;

    public SearchQuery(Class<E> returnType, PredicatesHolder predicates, int limit, Set<String> propertyKeys, StepDescriptor stepDescriptor) {
        super(predicates, stepDescriptor);
        this.returnType = returnType;
        this.limit = limit;
        this.propertyKeys = propertyKeys;
    }

    public Class<E> getReturnType(){
        return returnType;
    }

    public Set<String> getPropertyKeys() {
        return this.propertyKeys;
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
