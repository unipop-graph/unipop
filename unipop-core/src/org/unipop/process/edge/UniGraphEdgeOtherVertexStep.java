package org.unipop.process.edge;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.unipop.common.schema.referred.DeferredVertex;
import org.unipop.process.properties.PropertyFetcher;
import org.unipop.query.StepDescriptor;
import org.unipop.query.controller.ControllerManager;
import org.unipop.query.search.DeferredVertexQuery;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 6/8/16.
 */
public class UniGraphEdgeOtherVertexStep extends AbstractStep<Edge, Vertex> implements PropertyFetcher {
    private Iterator<Traverser.Admin<Vertex>> results = EmptyIterator.instance();
    private Set<String> propertyKeys;
    private List<DeferredVertexQuery.DefferedVertexController> deferredVertexControllers;

    public UniGraphEdgeOtherVertexStep(Traversal.Admin traversal, ControllerManager controllerManager) {
        super(traversal);
        this.propertyKeys = new HashSet<>();
        this.deferredVertexControllers = controllerManager.getControllers(DeferredVertexQuery.DefferedVertexController.class);
    }

    @Override
    public Set<String> getPropertyKeys() {
        return propertyKeys;
    }

    @Override
    public void fetchAllKeys() {
        this.propertyKeys = null;
    }

    @Override
    protected Traverser.Admin<Vertex> processNextStart() throws NoSuchElementException {
        while (!results.hasNext() && starts.hasNext())
            results = otherEdge();
        if (results.hasNext())
            return results.next();

        throw FastNoSuchElementException.instance();
    }

    private Iterator<Traverser.Admin<Vertex>> otherEdge() {
        List<Traverser.Admin<Vertex>> vertices = new ArrayList<>();
        while (this.starts.hasNext()) {
            Traverser.Admin<Edge> edge = starts.next();
            final List<Object> objects = edge.path().objects();
            for (int i = objects.size() - 2; i >= 0; i--) {
                if (objects.get(i) instanceof Vertex) {
                    Vertex vertex = ElementHelper.areEqual((Vertex) objects.get(i), edge.get().outVertex()) ?
                            edge.get().inVertex() :
                            edge.get().outVertex();
                    vertices.add(edge.split(vertex, this));
                }
            }
        }

        if (propertyKeys == null || propertyKeys.size() > 0){
            List<DeferredVertex> v = vertices.stream().map(Attachable::get).map(vertex -> ((DeferredVertex) vertex)).collect(Collectors.toList());
            DeferredVertexQuery query = new DeferredVertexQuery(v, propertyKeys, new StepDescriptor(this));
            deferredVertexControllers.forEach(deferredVertexController -> deferredVertexController.fetchProperties(query));
        }

        return vertices.iterator();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.PATH);
    }
}
