package org.unipop.process.edge;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.javatuples.Pair;
import org.unipop.process.UniPredicatesStep;
import org.unipop.process.order.Orderable;
import org.unipop.query.StepDescriptor;
import org.unipop.query.controller.ControllerManager;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.schema.reference.DeferredVertex;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;

public class UniGraphEdgeVertexStep extends UniPredicatesStep<Edge, Vertex> implements Orderable, Profiling {

    private Direction direction;
    private List<DeferredVertexQuery.DeferredVertexController> deferredVertexControllers;
    private StepDescriptor stepDescriptor;
    private List<Pair<String, Order>> orders;

    public UniGraphEdgeVertexStep(Traversal.Admin traversal, Direction direction, UniGraph graph, ControllerManager controllerManager) {
        super(traversal, graph);
        this.direction = direction;
        this.deferredVertexControllers = controllerManager.getControllers(DeferredVertexQuery.DeferredVertexController.class);
        this.stepDescriptor = new StepDescriptor(this);
    }

    @Override
    protected Iterator<Traverser.Admin<Vertex>> process(List<Traverser.Admin<Edge>> traversers) {
        List<Traverser.Admin<Vertex>> vertices = new ArrayList<>();
        traversers.forEach(travrser -> {
            travrser.get().vertices(direction).forEachRemaining(vertex -> vertices.add(travrser.split(vertex, this)));
        });

        if (propertyKeys == null || propertyKeys.size() > 1){
            List<DeferredVertex> v = vertices.stream().map(Attachable::get)
                    .filter(vertex -> vertex instanceof DeferredVertex)
                    .map(vertex -> ((DeferredVertex) vertex))
                    .filter(DeferredVertex::isDeferred)
                    .collect(Collectors.toList());
            if(v.size() > 0) {
                DeferredVertexQuery query = new DeferredVertexQuery(v, propertyKeys, orders, this.stepDescriptor);
                deferredVertexControllers.forEach(deferredVertexController -> deferredVertexController.fetchProperties(query));
            }
        }

        return vertices.iterator();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.direction);
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
