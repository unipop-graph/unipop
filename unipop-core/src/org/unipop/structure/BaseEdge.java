package org.unipop.structure;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.unipop.controller.EdgeController;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

public class BaseEdge extends BaseElement implements Edge {

    protected Vertex inVertex;
    protected Vertex outVertex;

    public BaseEdge(Map<String, Object> keyValues, Vertex outV, Vertex inV, final UniGraph graph) {
        super(keyValues, graph);
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
        ElementHelper.validateProperty(key, value);
        BaseProperty<V> vertexProperty = (BaseProperty<V>) addPropertyLocal(key, value);
        return vertexProperty;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        if(direction.equals(Direction.OUT)) return IteratorUtils.singletonIterator(outVertex);
        if(direction.equals(Direction.IN)) return IteratorUtils.singletonIterator(inVertex);
        return Arrays.asList(outVertex, inVertex).iterator();
    }


    @Override
    public Iterator<Property> properties(String... propertyKeys) {
        return propertyIterator(propertyKeys);
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }
}
