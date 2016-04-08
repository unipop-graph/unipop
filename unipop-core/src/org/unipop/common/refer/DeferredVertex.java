package org.unipop.common.refer;

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.structure.UniVertex;
import org.unipop.structure.UniGraph;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class DeferredVertex extends UniVertex {
    public DeferredVertex(Map<String, Object> properties, UniGraph graph) {
        super(properties, graph);
    }

    boolean deferred = true;

    private void checkDeferred() {
        if (deferred) {
            this.graph.getControllerManager().getControllers(DeferredVertexController.class).forEach(deferredController ->
                    deferredController.loadProperties(Iterators.singletonIterator(this)));
        }
    }

    public void loadProperties(Map<String, Object> properties) {
        deferred = false;
        if(properties != null)
            properties.forEach(this::addPropertyLocal);
    }

    @Override
    public VertexProperty property(VertexProperty.Cardinality cardinality, String key, Object value, Object... keyValues) {
        checkDeferred();
        return super.property(cardinality, key, value, keyValues);
    }


    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        checkDeferred();
        return super.edges(direction, edgeLabels);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        checkDeferred();
        return super.vertices(direction, edgeLabels);
    }

    @Override
    public VertexProperty property(String key, Object value) {
        checkDeferred();
        return super.property(key, value);
    }

    @Override
    public VertexProperty property(String key) {
        checkDeferred();
        return super.property(key);
    }

    @Override
    public Edge addEdge(String label, Vertex vertex, Object... keyValues) {
        checkDeferred();
        return super.addEdge(label, vertex, keyValues);
    }

    @Override
    public void remove() {
        checkDeferred();
        super.remove();
    }

    @Override
    public Iterator<VertexProperty> properties(String... propertyKeys) {
        checkDeferred();
        return super.properties(propertyKeys);
    }

    @Override
    public Set<String> keys() {
        checkDeferred();
        return super.keys();
    }

    @Override
    public Property createProperty(String key, Object value) {
        checkDeferred();
        return super.createProperty(key, value);
    }

    @Override
    public void removeProperty(Property property) {
        checkDeferred();
        super.removeProperty(property);
    }

}
