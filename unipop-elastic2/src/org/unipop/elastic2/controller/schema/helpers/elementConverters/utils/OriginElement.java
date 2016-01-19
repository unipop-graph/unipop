package org.unipop.elastic2.controller.schema.helpers.elementConverters.utils;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Created by Roman on 3/25/2015.
 */
public class OriginElement<E extends Element> implements ElementWrapper<E> {
    //region Constructor
    public OriginElement(E element) {
        this.element = element;
    }
    //endregion

    //region Wrapper Implementation
    @Override
    public Element wrap(E element) {
        this.element = element;
        return this;
    }

    @Override
    public E unwrap() {
        return this.element;
    }
    //endregion

    //region Element Implementation
    @Override
    public Object id() {
        return this.element.id();
    }

    @Override
    public String label() {
        return this.element.label();
    }

    @Override
    public Graph graph() {
        return this.element.graph();
    }

    @Override
    public Set<String> keys() {
        return this.element.keys();
    }

    @Override
    public <V> Property<V> property(String key) {
        return this.element.property(key);
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        return this.element.property(key, value);
    }

    @Override
    public <V> V value(String key) throws NoSuchElementException {
        return this.element.value(key);
    }

    @Override
    public void remove() {
        this.element.remove();
    }

    @Override
    public <V> Iterator<? extends Property<V>> properties(String... strings) {
        return null;
    }
    //endregion

    //region Properties
    public Object getOriginId() {
        return this.originId;
    }

    public void setOriginId(Object value) {
        this.originId = value;
    }
    //endregion

    //region Fields
    private E element;
    private Object originId;
    //endregion
}
