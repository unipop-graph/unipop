package org.unipop.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.*;
import org.unipop.controller.Predicates;

import java.util.*;

public abstract class BaseVertex extends BaseElement implements Vertex {

    protected BaseVertex(Object id, String label, UniGraph graph, Object[] keyValues) {
        super(id, label, graph, keyValues);
    }

    @Override
    public Property createProperty(String key, Object value) {
        return new BaseVertexProperty(this, key, value);
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... propertyKeys) {
        checkRemoved();
        if(propertyKeys != null && propertyKeys.length > 0) throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        return this.property(key, value);
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        return graph.traversal().V(this).toE(direction, edgeLabels);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        return graph.traversal().V(this).toE(direction, edgeLabels).toV(direction.opposite());
    }

    public void applyLazyFields(String label, Map<String, Object> properties) {
        setLabel(label);
        properties.entrySet().forEach((field) ->
                addPropertyLocal(field.getKey(), field.getValue()));
    }

    @Override
    public <V> VertexProperty<V> property(String key, V value) {
        checkRemoved();
        if(!Graph.Hidden.isHidden(key)) ElementHelper.validateProperty(key, value);
        BaseVertexProperty vertexProperty = (BaseVertexProperty) addPropertyLocal(key, value);
        innerAddProperty(vertexProperty);
        return vertexProperty;
    }

    protected abstract void innerAddProperty(BaseVertexProperty vertexProperty);

    @Override
    public <V> VertexProperty<V> property(final String key) {
        checkRemoved();
        if (this.properties.containsKey(key)) {
            return (VertexProperty<V>) this.properties.get(key);
        }
        else return VertexProperty.<V>empty();
    }

    @Override
    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        if (null == vertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        checkRemoved();
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);

        BaseEdge edge = graph.getControllerProvider().addEdge(idValue, label, this, vertex, keyValues);
        return edge;
    }

    @Override
    public void remove() {
        super.remove();
        Iterator<Edge> edges = edges(Direction.BOTH);
        edges.forEachRemaining(edge-> {
            edge.remove();
        });
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        checkRemoved();
        return innerPropertyIterator(propertyKeys);
    }

    @Override
    protected void checkRemoved() {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id);
    }

    public Iterator<BaseEdge> cachedEdges(Direction direction, String[] edgeLabels, Predicates predicates) {
        return null;
    }
}