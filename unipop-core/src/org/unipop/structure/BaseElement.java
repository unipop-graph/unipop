package org.unipop.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.unipop.controller.ElementController;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public abstract class BaseElement<C extends ElementController> implements Element{
    protected HashMap<String, Property> properties = new HashMap<>();
    protected final Object id;
    protected String label;
    protected final UniGraph graph;
    protected C controller;
    public boolean removed = false;

    public BaseElement(final Object id, final String label, UniGraph graph, Map<String, Object> keyValues, C controller) {
        this.graph = graph;
        this.controller = controller;
        this.id = id != null ? id.toString() : new com.eaio.uuid.UUID().toString();
        this.label = label;
        if(keyValues != null)
            keyValues.forEach(this::addPropertyLocal);
    }

    public Property addPropertyLocal(String key, Object value) {
        if(key == null) return null;

        ElementHelper.validateProperty(key, value);
        Property property = createProperty(key, value);
        properties.put(key, property);
        return property;
    }

    @Override
    public Object id() {
        return this.id;
    }

    @Override
    public String label() {
        return this.label;
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public Set<String> keys() {
        return this.properties.keySet();
    }

    @Override
    public <V> Property<V> property(final String key) {
        return this.properties.containsKey(key) ? this.properties.get(key) : Property.<V>empty();
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    protected Iterator propertyIterator(String[] propertyKeys) {
        HashMap<String, Property> properties = (HashMap<String, Property>) this.properties.clone();

        if (propertyKeys.length > 0)
            return properties.entrySet().stream().filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys)).map(x -> x.getValue()).iterator();

        return properties.values().iterator();
    }


    public void removeProperty(Property property) {
        properties.remove(property.key());
        controller.removeProperty(this, property);
    }

    protected abstract Property createProperty(String key, Object value);

    @Override
    public void remove() {
        controller.remove(this);
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public UniGraph getGraph() {
        return graph;
    }
}
