package org.unipop.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.*;

import java.util.Collections;
import java.util.Iterator;

public class UniVertexProperty<V> implements VertexProperty<V> {

    private final UniVertex vertex;
    private final String key;
    private final V value;

    public UniVertexProperty(final UniVertex vertex, final String key, final V value) {
        this.vertex = vertex;
        this.key = key;
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
        // VertexProperty.properties() returns this vertex-property's meta-properties.
        // Unipop does not support meta-properties (UniFeatures.supportsMetaProperties() == false),
        // so there are none to return. Per the TinkerPop contract (cf. ReferenceVertexProperty),
        // a graph without meta-properties returns an empty iterator here rather than throwing —
        // the feature harness detaches every result vertex via this method, so throwing would
        // abort otherwise-valid scenarios.
        return Collections.emptyIterator();
    }

    @Override
    public void remove() {
        vertex.removeProperty(this);
    }
}