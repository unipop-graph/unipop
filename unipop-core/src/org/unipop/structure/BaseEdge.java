package org.unipop.structure;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.mockito.internal.util.collections.ListUtil;

import java.util.*;

public abstract class BaseEdge extends BaseElement implements Edge {

    protected Vertex inVertex;
    protected Vertex outVertex;

    public BaseEdge(final Object id, final String label, Object[] keyValues, Vertex outV, Vertex inV, final UniGraph graph) {
        super(id, label, graph, keyValues);
        ElementHelper.validateLabel(label);
        this.outVertex = outV;
        this.inVertex = inV;
    }

    @Override
    public  Property createProperty(String key, Object value) {
        return new BaseProperty<>(this, key, value);
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        checkRemoved();
        if(!Graph.Hidden.isHidden(key)) ElementHelper.validateProperty(key, value);
        BaseProperty<V> vertexProperty = (BaseProperty<V>) addPropertyLocal(key, value);
        innerAddProperty(vertexProperty);
        return vertexProperty;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        checkRemoved();
        if(direction.equals(Direction.OUT)) return IteratorUtils.singletonIterator(outVertex);
        if(direction.equals(Direction.IN)) return IteratorUtils.singletonIterator(inVertex);
        return Arrays.asList(outVertex, inVertex).iterator();
    }


    protected abstract void innerAddProperty(BaseProperty vertexProperty);

    @Override
    public Iterator<Property> properties(String... propertyKeys) {
        checkRemoved();
        return innerPropertyIterator(propertyKeys);
    }

    @Override
    protected void checkRemoved() {
        if (this.removed)
            throw Element.Exceptions.elementAlreadyRemoved(Edge.class, this.id);
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }
}
