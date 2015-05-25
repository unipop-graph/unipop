package org.elasticgremlin.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.*;

public abstract class ElasticElement implements Element{
    protected HashMap<String, Property> properties = new HashMap();
    protected final Object id;
    protected String label;
    protected final ElasticGraph graph;
    protected boolean removed = false;

    public ElasticElement(final Object id, final String label, ElasticGraph graph, Object[] keyValues) {
        this.graph = graph;
        this.id = id != null ? id : new com.eaio.uuid.UUID().toString();
        this.label = label;
        if (keyValues != null) {
            if(keyValues.length % 2 == 1) throw Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo();
            for (int i = 0; i < keyValues.length; i = i + 2) {
                String key = keyValues[i].toString();
                Object value = keyValues[i + 1];

                addPropertyLocal(key, value);
            }
        }

    }

    public Property addPropertyLocal(String key, Object value) {
        checkRemoved();
        if (shouldAddProperty(key)) {
            ElementHelper.validateProperty(key, value);
            Property property = createProperty(key, value);
            properties.put(key, property);
            return property;
        }
        return null;
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
        checkRemoved();
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

    protected Iterator innerPropertyIterator(String[] propertyKeys) {
        HashMap<String, Property> properties = (HashMap<String, Property>) this.properties.clone();

        if (propertyKeys.length > 0)
            return properties.entrySet().stream().filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys)).map(x -> x.getValue()).iterator();

        return (Iterator) properties.values().iterator();
    }


    public void removeProperty(Property property) {
        properties.remove(property.key());
        try {
            graph.elasticService.addElement(this, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected abstract Property createProperty(String key, Object value);

    protected boolean shouldAddProperty(String key) {
        return !key.equals("label") && !key.equals("id");
    }

    protected abstract void checkRemoved();


    public Map<String, Object> allFields() {
        Map<String, Object> map = new HashMap<>();
        properties.forEach((key, value) -> map.put(key, value.value()));
        return map;
    }
}
