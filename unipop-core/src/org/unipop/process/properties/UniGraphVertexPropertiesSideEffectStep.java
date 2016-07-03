package org.unipop.process.properties;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.unipop.process.UniBulkStep;
import org.unipop.schema.reference.DeferredVertex;
import org.unipop.query.StepDescriptor;
import org.unipop.query.controller.ControllerManager;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;

public class UniGraphVertexPropertiesSideEffectStep extends UniBulkStep<Vertex, Vertex> {

    private final List<DeferredVertexQuery.DeferredVertexController> controllers;
    private StepDescriptor stepDescriptor;

    public UniGraphVertexPropertiesSideEffectStep(Traversal.Admin traversal, ControllerManager controllerManager, UniGraph graph) {
        super(traversal, graph);
        this.stepDescriptor = new StepDescriptor(this);
        this.controllers = controllerManager.getControllers(DeferredVertexQuery.DeferredVertexController.class);
    }

    @Override
    protected Iterator<Traverser.Admin<Vertex>> process(List<Traverser.Admin<Vertex>> traversers) {
        List<DeferredVertex> deferredVertices = traversers.stream().map(Attachable::get)
                .filter(vertex -> vertex instanceof DeferredVertex)
                .map(vertex -> ((DeferredVertex) vertex))
                .filter(DeferredVertex::isDeferred)
                .collect(Collectors.toList());

        if (deferredVertices.size() > 0) {
            DeferredVertexQuery query = new DeferredVertexQuery(deferredVertices, null, this.stepDescriptor);
            controllers.forEach(controller -> controller.fetchProperties(query));
        }

        return traversers.iterator();
    }
}
