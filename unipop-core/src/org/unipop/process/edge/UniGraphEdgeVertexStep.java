package org.unipop.process.edge;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
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
public class UniGraphEdgeVertexStep extends AbstractStep<Edge, Vertex> implements PropertyFetcher {

    private Direction direction;
    private Set<String> propertyKeys;
    private Iterator<Traverser.Admin<Vertex>> results = EmptyIterator.instance();
    private List<DeferredVertexQuery.DefferedVertexController> deferredVertexControllers;

    public UniGraphEdgeVertexStep(Traversal.Admin traversal, Direction direction, ControllerManager controllerManager) {
        super(traversal);
        this.direction = direction;
        this.propertyKeys = new HashSet<>();
        propertyKeys.add("id");
        this.deferredVertexControllers = controllerManager.getControllers(DeferredVertexQuery.DefferedVertexController.class);
    }

    @Override
    protected Traverser.Admin<Vertex> processNextStart() throws NoSuchElementException {
        while (!results.hasNext() && starts.hasNext())
            results = edgeVertex();
        if (results.hasNext())
            return results.next();

        throw FastNoSuchElementException.instance();
    }

    private Iterator<Traverser.Admin<Vertex>> edgeVertex() {
        List<Traverser.Admin<Vertex>> vertices = new ArrayList<>();
        while (this.starts.hasNext()) {
            Traverser.Admin<Edge> edge = starts.next();
            edge.get().vertices(direction).forEachRemaining(vertex -> vertices.add(edge.split(vertex, this)));
        }

        if (propertyKeys == null || propertyKeys.size() > 1){
            List<DeferredVertex> v = vertices.stream().map(Attachable::get).map(vertex -> ((DeferredVertex) vertex)).collect(Collectors.toList());
            DeferredVertexQuery query = new DeferredVertexQuery(v, propertyKeys, new StepDescriptor(this));
            deferredVertexControllers.forEach(deferredVertexController -> deferredVertexController.fetchProperties(query));
        }

        return vertices.iterator();
    }

    @Override
    public Set<String> getPropertyKeys() {
        return propertyKeys;
    }

    @Override
    public void fetchAllKeys() {
        propertyKeys = null;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.direction);
    }
}
