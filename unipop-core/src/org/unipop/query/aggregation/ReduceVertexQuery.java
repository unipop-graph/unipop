package org.unipop.query.aggregation;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.StepDescriptor;
import org.unipop.query.controller.UniQueryController;
import org.unipop.query.VertexQuery;

import java.util.List;

public class ReduceVertexQuery extends ReduceQuery implements VertexQuery {

    private final List<Vertex> vertices;
    private final Direction direction;

    public ReduceVertexQuery(List<Vertex> vertices, Direction direction, PredicatesHolder predicates, StepDescriptor stepDescriptor) {
        super(predicates, stepDescriptor);
        this.vertices = vertices;
        this.direction = direction;
    }

    @Override
    public List<Vertex> getVertices() {
        return vertices;
    }

    @Override
    public Direction getDirection() {
        return direction;
    }

    public interface ReduceVertexController extends UniQueryController {
        void query(ReduceVertexQuery uniQuery);
    }

    @Override
    public String toString() {
        return "ReduceVertexQuery{" +
                "vertices=" + vertices +
                ", direction=" + direction +
                '}';
    }
}
