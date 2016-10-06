package org.unipop.query.aggregation;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.StepDescriptor;
import org.unipop.query.controller.UniQueryController;
import org.unipop.query.VertexQuery;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class ReduceVertexQuery extends ReduceQuery implements VertexQuery {

    private final List<Vertex> vertices;
    private final Direction direction;
    private final boolean returnsVertex;

    public ReduceVertexQuery(boolean returnsVertex, List<Vertex> vertices, Direction direction, PredicatesHolder predicates, Set<String> properties, String reduceOn, ReduceOperator op, int limit, StepDescriptor stepDescriptor) {
        super(predicates, properties, reduceOn, op, Edge.class, limit, stepDescriptor);
    public ReduceVertexQuery(List<Vertex> vertices, Direction direction, PredicatesHolder predicates, StepDescriptor stepDescriptor, Traversal traversal) {
        super(predicates, stepDescriptor, traversal);
        this.vertices = vertices;
        this.direction = direction;
        this.returnsVertex = returnsVertex;
    }

    @Override
    public List<Vertex> getVertices() {
        return vertices;
    }

    public boolean isReturnsVertex() {
        return returnsVertex;
    }

    @Override
    public Direction getDirection() {
        return direction;
    }

    public interface ReduceVertexController extends UniQueryController {
        Iterator<Object> reduce(ReduceVertexQuery query);
    }

    @Override
    public String toString() {
        return "ReduceVertexQuery{" +
                "vertices=" + vertices +
                ", direction=" + direction +
                '}';
    }
}
