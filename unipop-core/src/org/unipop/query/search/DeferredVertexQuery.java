package org.unipop.query.search;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
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
import java.util.stream.Collectors;

public class DeferredVertexQuery extends SearchQuery<Vertex> {
    private List<DeferredVertex> vertices;

    public DeferredVertexQuery(List<DeferredVertex> vertices, Set<String> propertyKeys, List<Pair<String, Order>> orders, StepDescriptor stepDescriptor) {
        super(Vertex.class, PredicatesHolderFactory.empty(), -1, propertyKeys, orders, stepDescriptor);
        this.vertices = vertices;
    }

    public List<DeferredVertex> getVertices() {
        return this.vertices;
    }

    @Override
    public PredicatesHolder getPredicates() {
        Set<Object> ids = vertices.stream().map(Element::id).collect(Collectors.toSet());
        Set<Object> labels = vertices.stream().map(Element::label).collect(Collectors.toSet());
        HasContainer id = new HasContainer(T.id.getAccessor(), P.within(ids));
        HasContainer label = new HasContainer(T.label.getAccessor(), P.within(labels));
        return PredicatesHolderFactory
                .createFromPredicates(PredicatesHolder.Clause.And, Sets.newHashSet(id, label));
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
