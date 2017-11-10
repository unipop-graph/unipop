package org.unipop.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.*;

import java.util.Iterator;
import java.util.Map;

public class UniVertexProperty<V> implements VertexProperty<V> {

    private final UniVertex vertex;
    private final String key;
    private V value;
    private Map<String, Object> properties;

    public UniVertexProperty(final UniVertex vertex, final String key, final V value) {
        this.vertex = vertex;
        this.key = key;
        if (value instanceof Map &&
                ((Map) value).containsKey("properties")) {
            this.properties = (Map<String, Object>) ((Map) value).get("properties");
            this.value = (V) (((Map) value).get("value"));
        }
        this.value = value;
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public Object id() {
        return (long) (this.key.hashCode() + this.value.hashCode() + this.vertex.id().hashCode());
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Element) this);
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        throw VertexProperty.Exceptions.multiPropertiesNotSupported();
    }

    @Override
    public Vertex element() {
        return this.vertex;
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        throw VertexProperty.Exceptions.multiPropertiesNotSupported();
    }

    @Override
    public void remove() {
        vertex.removeProperty(this);
    }
}