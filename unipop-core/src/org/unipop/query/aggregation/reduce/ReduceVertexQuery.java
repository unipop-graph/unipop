package org.unipop.query.aggregation.reduce;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.StepDescriptor;
import org.unipop.query.VertexQuery;
import org.unipop.query.controller.UniQueryController;
import org.unipop.query.predicates.PredicatesHolder;

import java.util.List;

/**
 * @author Gur Ronen
 * @since 6/27/2016
 */
public class ReduceVertexQuery extends ReduceQuery implements VertexQuery {
    private final List<Vertex> vertices;
    private final Direction direction;

    public ReduceVertexQuery(PredicatesHolder predicates, StepDescriptor stepDescriptor, List<Vertex> vertices, Direction direction) {
        super(predicates, stepDescriptor, op, columnName);
        this.vertices = vertices;
        this.direction = direction;
    }

    @Override
    public List<Vertex> getVertices() {
        return this.vertices;
    }

    @Override
    public Direction getDirection() {
        return this.direction;
    }

    public interface ReduceVertexController extends UniQueryController {
        Number reduce(ReduceVertexQuery uniQuery);
    }

}
