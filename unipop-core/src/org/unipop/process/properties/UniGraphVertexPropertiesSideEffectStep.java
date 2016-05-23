package org.unipop.process.properties;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ExpandableStepIterator;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.unipop.common.schema.referred.DeferredVertex;
import org.unipop.query.controller.ControllerManager;
import org.unipop.query.search.DeferredVertexQuery;

import java.util.*;
import java.util.stream.Collectors;

public class UniGraphVertexPropertiesSideEffectStep extends AbstractStep<Vertex, Vertex> {

    private final int bulk;
    private ControllerManager controllerManager;
    private Iterator<Traverser.Admin<Vertex>> results = EmptyIterator.instance();

    public UniGraphVertexPropertiesSideEffectStep(Traversal.Admin traversal, ControllerManager controllerManager) {
        super(traversal);
        this.controllerManager = controllerManager;
        this.bulk = getTraversal().getGraph().get().configuration().getInt("bulk", 100);
    }

    @Override
    protected Traverser.Admin<Vertex> processNextStart() throws NoSuchElementException {
        while (!results.hasNext() && starts.hasNext())
            results = query(starts);
        if(results.hasNext())
            return results.next();

        throw FastNoSuchElementException.instance();
    }

    private Iterator<Traverser.Admin<Vertex>> query(ExpandableStepIterator<Vertex> traversers) {
        List<Traverser.Admin<Vertex>> copyTraversers = new ArrayList<>();
        while (traversers.hasNext() && copyTraversers.size() <= bulk)
            copyTraversers.add(traversers.next());

        List<DeferredVertex> deferredVertices = copyTraversers.stream()
                .filter(traverser -> traverser.get() instanceof DeferredVertex)
                .<DeferredVertex>map(traverser -> (DeferredVertex)traverser.get())
                .filter(DeferredVertex::isDeferred)
                .collect(Collectors.toList());

        if (deferredVertices.size() > 0) {
            DeferredVertexQuery query = new DeferredVertexQuery(deferredVertices, null);
            controllerManager.getControllers(DeferredVertexQuery.DefferedVertexController.class)
                    .forEach(controller -> controller.fetchProperties(query));
        }

        return copyTraversers.iterator();
    }
}
