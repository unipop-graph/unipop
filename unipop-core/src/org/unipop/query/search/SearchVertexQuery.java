package org.unipop.query.search;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.StepDescriptor;
import org.unipop.query.controller.UniQueryController;
import org.unipop.query.VertexQuery;

import java.util.Iterator;
import java.util.List;

public class SearchVertexQuery extends SearchQuery<Edge> implements VertexQuery {

    private final List<Vertex> vertices;
    private final Direction direction;

    public SearchVertexQuery(Class<Edge> returnType, List<Vertex> vertices, Direction direction, PredicatesHolder predicates, int limit, StepDescriptor stepDescriptor) {
        super(returnType, predicates, limit, stepDescriptor);
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

    public interface SearchVertexController extends UniQueryController {
        Iterator<Edge> search(SearchVertexQuery uniQuery);
    }
}
