package org.unipop.structure;

import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.*;

import java.util.*;

public class UniVertexProperty<V> extends UniElement implements VertexProperty<V> {

    private final UniVertex vertex;
    private final String key;
    private final V value;

    public UniVertexProperty(final UniVertex vertex, final String key, final V value) {
        super(new HashMap<String, Object>(){{put(T.id.getAccessor(), UUID.randomUUID());put(T.label.getAccessor(), key);}}, vertex.graph);
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
    protected Map<String, Property> getPropertiesMap() {
        throw new NotImplementedException();
    }

    @Override
    protected String getDefaultLabel() {
        return null;
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
    protected Property createProperty(String key, Object value) {
        return null;
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Element) this);
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();

    }

    @Override
    public Vertex element() {
        return this.vertex;
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public void remove() {
        vertex.removeProperty(this);
    }
}
