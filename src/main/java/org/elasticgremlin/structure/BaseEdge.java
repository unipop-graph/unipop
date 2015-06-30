package org.elasticgremlin.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public abstract class BaseEdge extends BaseElement implements Edge {

    protected Vertex inVertex;
    protected Vertex outVertex;

    public BaseEdge(final Object id, final String label, Object[] keyValues, final ElasticGraph graph) {
        super(id, label, graph, keyValues);
        ElementHelper.validateLabel(label);
    }

    @Override
    public <V> Property<V> createProperty(String key, V value) {
        return new BaseProperty<>(this, key, value);
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        checkRemoved();
        ElementHelper.validateProperty(key, value);
        BaseProperty<V> vertexProperty = (BaseProperty<V>) addPropertyLocal(key, value);
        innerAddProperty(vertexProperty);
        return vertexProperty;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        checkRemoved();
        ArrayList<Vertex> vertices = new ArrayList<>();
        if(direction.equals(Direction.OUT) || direction.equals(Direction.BOTH))
            vertices.add(outVertex);
        if(direction.equals(Direction.IN) || direction.equals(Direction.BOTH))
            vertices.add(inVertex);
        return vertices.iterator();
    }

    protected abstract void innerAddProperty(BaseProperty vertexProperty);

    @Override
    public Iterator<Property> properties(String... propertyKeys) {
        checkRemoved();
        return innerPropertyIterator(propertyKeys);
    }

    @Override
    public void remove() {
        notifyVertices();
        super.remove();
    }

    protected void notifyVertices() {
        ArrayList<BaseVertex> vertices = new ArrayList<>(2);
        if (inVertex != null && BaseVertex.class.isAssignableFrom(inVertex.getClass())) {
            vertices.add((BaseVertex) inVertex);
        }
        if (outVertex != null && BaseVertex.class.isAssignableFrom(outVertex.getClass())) {
            vertices.add((BaseVertex) outVertex);
        }
        vertices.forEach(vertex -> vertex.removeEdge(this));
    }

    @Override
    protected void checkRemoved() {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Edge.class, this.id);
    }
}
