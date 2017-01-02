package org.unipop.query.aggregation;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.javatuples.Pair;
import org.unipop.query.StepDescriptor;
import org.unipop.query.UniQuery;
import org.unipop.query.controller.UniQueryController;
import org.unipop.query.search.SearchQuery;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by sbarzilay on 9/4/16.
 */
public class LocalQuery<S extends Element> extends UniQuery{
    // TODO: should add `bottom` aggregation to support reducingQuery
    private List<S> elements;
    private SearchQuery<S> searchQuery;
    private Class queryClass;

    public LocalQuery(Class queryClass, List<S> elements, SearchQuery<S> searchQuery, StepDescriptor stepDescriptor) {
        super(stepDescriptor);
        this.elements = elements;
        this.searchQuery = searchQuery;
        this.queryClass = queryClass;
    }

    public List<S> getElements() {
        return elements;
    }

    public Class getQueryClass() {
        return queryClass;
    }

    public SearchQuery<S> getSearchQuery() {
        return searchQuery;
    }

    public Traversal.Admin getTraversal() {
        StepDescriptor stepDescriptor = getStepDescriptor();
        if (stepDescriptor == null)
            return null;
        return stepDescriptor.getTraversal();
    }

    public interface LocalController extends UniQueryController {
        <S extends Element> Iterator<Pair<String, S>> local(LocalQuery<S> query);
    }
}