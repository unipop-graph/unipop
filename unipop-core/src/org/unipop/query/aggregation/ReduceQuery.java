package org.unipop.query.aggregation;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.StepDescriptor;
import org.unipop.query.controller.UniQueryController;
import org.unipop.query.search.SearchQuery;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class ReduceQuery extends SearchQuery {
    private ReduceOperator op;
    private String reduceOn;
    private List<Vertex> vertices;

    public ReduceQuery(PredicatesHolder predicates, Set<String> properties, String reduceOn, ReduceOperator op, Class returnType, int limit, StepDescriptor stepDescriptor) {
        super(returnType, predicates, limit, properties, Collections.emptyList(), stepDescriptor);
        this.op = op;
        this.reduceOn = reduceOn;
        this.vertices = Collections.emptyList();
    }
    public ReduceQuery(List<Vertex> vertices, PredicatesHolder predicates, Set<String> properties, String reduceOn, ReduceOperator op, Class returnType, int limit, StepDescriptor stepDescriptor) {
        super(returnType, predicates, limit, properties, Collections.emptyList(), stepDescriptor);
        this.op = op;
        this.reduceOn = reduceOn;
        this.vertices = vertices;
    public ReduceQuery(PredicatesHolder predicates, StepDescriptor stepDescriptor, Traversal traversal) {
        super(predicates, stepDescriptor, traversal);
    }

    public interface ReduceController<E extends Element> extends UniQueryController {
        Iterator<Object> reduce(ReduceQuery query);
    }

    public List<Vertex> getVertices() {
        return vertices;
    }

    public String getReduceOn() {
        return reduceOn;
    }

    public ReduceOperator getOp() {
        return op;
    }

    public enum ReduceOperator{
        Max,
        Min,
        Count,
        Sum,
        Mean
    }
}
