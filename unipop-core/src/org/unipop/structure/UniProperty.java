package org.unipop.structure;


import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.*;

public class UniProperty<V> implements Property<V> {
    protected final UniElement element;
    protected final String key;
    protected V value;
    protected final UniGraph graph;

    public UniProperty(UniElement element, String key, V value) {
        ElementHelper.validateProperty(key, value);
        this.element = element;
        this.key = key;
        this.value = value;
        this.graph = this.element.graph;
    }

    @Override
    public Element element() {
        return this.element;
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
        return null != this.value;
    }

    public String toString() {
        return StringFactory.propertyString(this);
    }

    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public void remove() {
        element.removeProperty(this);
    }
}
