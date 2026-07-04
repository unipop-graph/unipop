package org.unipop.process.edge;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.javatuples.Pair;
import org.unipop.process.UniPredicatesStep;
import org.unipop.process.order.Orderable;
import org.unipop.query.StepDescriptor;
import org.unipop.query.controller.ControllerManager;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.schema.reference.DeferredVertex;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;

public class UniGraphEdgeOtherVertexStep extends UniPredicatesStep<Edge, Vertex> implements Orderable, Profiling {
    private List<DeferredVertexQuery.DeferredVertexController> deferredVertexControllers;
    private StepDescriptor stepDescriptor;
    private List<Pair<String, Order>> orders;
    private PredicatesHolder vertexPredicates = PredicatesHolderFactory.empty();

    public void setVertexPredicates(PredicatesHolder vertexPredicates) {
        this.vertexPredicates = vertexPredicates == null ? PredicatesHolderFactory.empty() : vertexPredicates;
    }

    public UniGraphEdgeOtherVertexStep(Traversal.Admin traversal, UniGraph graph, ControllerManager controllerManager) {
        super(traversal, graph);
        this.deferredVertexControllers = controllerManager.getControllers(DeferredVertexQuery.DeferredVertexController.class);
        this.stepDescriptor = new StepDescriptor(this);
    }

    @Override
    protected Iterator<Traverser.Admin<Vertex>> process(List<Traverser.Admin<Edge>> traversers) {
        List<Traverser.Admin<Vertex>> vertices = new ArrayList<>();
        traversers.forEach(traverser -> {
            final List<Object> objects = traverser.path().objects();
            if (objects.get(objects.size()-2) instanceof Vertex) {
                Vertex vertex = ElementHelper.areEqual((Vertex) objects.get(objects.size()-2), traverser.get().outVertex()) ?
                        traverser.get().inVertex() :
                        traverser.get().outVertex();
                vertices.add(traverser.split(vertex, this));
            }
        });

        boolean fetch = this.vertexPredicates.notEmpty() || propertyKeys == null || propertyKeys.size() > 1;
        if (fetch) {
            List<DeferredVertex> v = vertices.stream().map(Attachable::get)
                    .filter(vertex -> vertex instanceof DeferredVertex)
                    .map(vertex -> ((DeferredVertex) vertex))
                    .filter(DeferredVertex::isDeferred)
                    .collect(Collectors.toList());
            if (v.size() > 0) {
                Set<String> fetchKeys = (this.vertexPredicates.notEmpty() && propertyKeys != null && propertyKeys.isEmpty())
                        ? null : propertyKeys;
                DeferredVertexQuery query = new DeferredVertexQuery(v, this.vertexPredicates, fetchKeys, orders, this.stepDescriptor, traversal);
                deferredVertexControllers.forEach(deferredVertexController -> deferredVertexController.fetchProperties(query));
            }
        }
        if (!this.vertexPredicates.notEmpty()) return vertices.iterator();
        // See UniGraphVertexStep: one DeferredVertex per incident edge means a matched id may have
        // several instances but the fetch un-defers only one. Keep every traverser whose vertex id
        // matched, dropping only unmatched ids.
        Set<Object> matchedIds = vertices.stream().map(Attachable::get)
                .filter(x -> x instanceof DeferredVertex && !((DeferredVertex) x).isDeferred())
                .map(x -> ((DeferredVertex) x).id())
                .collect(Collectors.toSet());
        return vertices.stream()
                .filter(t -> { Vertex x = t.get(); return !(x instanceof DeferredVertex) || matchedIds.contains(((DeferredVertex) x).id()); })
                .iterator();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.PATH);
    }

    @Override
    public void setMetrics(MutableMetrics metrics) {
        this.stepDescriptor = new StepDescriptor(this, metrics);
    }

    @Override
    public void setOrders(List<Pair<String, Order>> orders) {
        this.orders = orders;
    }
}
