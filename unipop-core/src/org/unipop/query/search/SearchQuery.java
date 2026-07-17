package org.unipop.query.search;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.javatuples.Pair;
import org.unipop.query.StepDescriptor;
import org.unipop.query.controller.UniQueryController;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.query.predicates.PredicatesHolder;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class SearchQuery<E extends Element> extends PredicateQuery<E> {
    private final Class<E> returnType;
    private final int limit;
    private Set<String> propertyKeys;
    private List<Pair<String, Order>> orders;
    private int offset = 0;
    private int pushedOffset = 0;

    public SearchQuery(Class<E> returnType, PredicatesHolder predicates, int limit, Set<String> propertyKeys, List<Pair<String, Order>> orders, StepDescriptor stepDescriptor, Traversal traversal) {
        super(predicates, stepDescriptor, traversal);
        this.returnType = returnType;
        this.limit = limit;
        this.propertyKeys = propertyKeys;
        this.orders = orders;
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

    public List<Pair<String, Order>> getOrders() {
        return orders;
    }

    public int getOffset() { return offset; }
    public void setOffset(int offset) { this.offset = offset; }
    public int getPushedOffset() { return pushedOffset; }
    public void setPushedOffset(int pushedOffset) { this.pushedOffset = pushedOffset; }

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
