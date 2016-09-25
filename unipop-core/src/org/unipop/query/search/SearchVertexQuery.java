package org.unipop.query.search;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.StepDescriptor;
import org.unipop.query.controller.UniQueryController;
import org.unipop.query.VertexQuery;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class SearchVertexQuery extends SearchQuery<Edge> implements VertexQuery {

    private final List<Vertex> vertices;
    private final Direction direction;

    public SearchVertexQuery(Class<Edge> returnType, List<Vertex> vertices, Direction direction, PredicatesHolder predicates, int limit, Set<String> propertyKeys, List<Pair<String, Order>> orders, StepDescriptor stepDescriptor) {
        super(returnType, predicates, limit, propertyKeys, orders, stepDescriptor);
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

    @Override
    public boolean test(Edge element, PredicatesHolder predicates) {
        boolean edgePredicates = super.test(element, predicates);
        if (!edgePredicates) return false;
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (vertices.contains(element.outVertex())) return true;
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (vertices.contains(element.inVertex())) return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "SearchVertexQuery{" +
                "vertices=" + vertices +
                ", direction=" + direction +
                '}';
    }
}
