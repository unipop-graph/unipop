package org.unipop.query.search;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.schema.reference.DeferredVertex;
import org.unipop.query.StepDescriptor;
import org.unipop.query.UniQuery;
import org.unipop.query.controller.UniQueryController;

import java.util.List;
import java.util.Set;

public class DeferredVertexQuery extends SearchQuery<Vertex> {
    private List<DeferredVertex> vertices;

    public DeferredVertexQuery(List<DeferredVertex> vertices, Set<String> propertyKeys, List<Pair<String, Order>> orders, StepDescriptor stepDescriptor) {
        super(Vertex.class, PredicatesHolderFactory.empty(), -1, propertyKeys, orders, stepDescriptor);
        this.vertices = vertices;
    }

    public List<DeferredVertex> getVertices() {
        return this.vertices;
    }

    public interface DeferredVertexController extends UniQueryController {
        void fetchProperties(DeferredVertexQuery query);
    }

    @Override
    public String toString() {
        return "DeferredVertexQuery{" +
                "vertices=" + vertices +
                '}';
    }
}
