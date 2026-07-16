package org.unipop.query.search;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.unipop.query.StepDescriptor;
import org.unipop.query.VertexQuery;
import org.unipop.query.controller.UniQueryController;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class SearchVertexQuery extends SearchQuery<Edge> implements VertexQuery {

    private final List<Vertex> vertices;
    private final Direction direction;
    private final PredicatesHolder targetPredicates;
    private final List<Pair<String, Order>> targetOrders;
    private final int targetLimit;
    private final boolean hydrateTarget;

    // Backward-compatible: edge-return / callers with no target intent.
    public SearchVertexQuery(Class<Edge> returnType, List<Vertex> vertices, Direction direction, PredicatesHolder predicates, int limit, Set<String> propertyKeys, List<Pair<String, Order>> orders, StepDescriptor stepDescriptor, Traversal traversal) {
        this(returnType, vertices, direction, predicates, limit, propertyKeys, orders,
                PredicatesHolderFactory.empty(), null, -1, false, stepDescriptor, traversal);
    }

    // Target-aware constructor with default stepDescriptor/traversal (for test compatibility)
    public SearchVertexQuery(Class<Edge> returnType, List<Vertex> vertices, Direction direction, PredicatesHolder predicates, int limit, Set<String> propertyKeys, List<Pair<String, Order>> orders, PredicatesHolder targetPredicates, List<Pair<String, Order>> targetOrders, int targetLimit, boolean hydrateTarget) {
        this(returnType, vertices, direction, predicates, limit, propertyKeys, orders, targetPredicates, targetOrders, targetLimit, hydrateTarget, null, null);
    }

    public SearchVertexQuery(Class<Edge> returnType, List<Vertex> vertices, Direction direction, PredicatesHolder predicates, int limit, Set<String> propertyKeys, List<Pair<String, Order>> orders, PredicatesHolder targetPredicates, List<Pair<String, Order>> targetOrders, int targetLimit, boolean hydrateTarget, StepDescriptor stepDescriptor, Traversal traversal) {
        super(returnType, predicates, limit, propertyKeys, orders, stepDescriptor, traversal);
        this.vertices = vertices;
        this.direction = direction;
        this.targetPredicates = targetPredicates == null ? PredicatesHolderFactory.empty() : targetPredicates;
        this.targetOrders = targetOrders;
        this.targetLimit = targetLimit;
        this.hydrateTarget = hydrateTarget;
    }

    public PredicatesHolder getTargetPredicates() {
        return targetPredicates;
    }

    public List<Pair<String, Order>> getTargetOrders() {
        return targetOrders;
    }

    public int getTargetLimit() {
        return targetLimit;
    }

    public boolean isHydrateTarget() {
        return hydrateTarget;
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
