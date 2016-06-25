package org.unipop.structure;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.unipop.query.mutation.PropertyQuery;
import org.unipop.query.mutation.RemoveQuery;

import java.util.*;

public abstract class UniElement implements Element{
    protected HashMap<String, Property> properties = new HashMap<>();
    protected String id;
    protected String label;
    protected UniGraph graph;

    public UniElement(Map<String, Object> properties, UniGraph graph) {
        this.graph = graph;

        this.id = ObjectUtils.firstNonNull(
                properties.remove(T.id.getAccessor()),
                properties.remove(T.id.toString()),
                new com.eaio.uuid.UUID())
                .toString();

        this.label = ObjectUtils.firstNonNull(
                properties.remove(T.label.getAccessor()),
                properties.remove(T.label.toString()),
                getDefaultLabel())
                .toString();

        properties.forEach(this::addPropertyLocal);
    }

    protected abstract String getDefaultLabel();

    protected Property addPropertyLocal(String key, Object value) {
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
        PropertyQuery<UniElement> propertyQuery = new PropertyQuery<>(this, property, PropertyQuery.Action.Remove, null);
        this.graph.getControllerManager().getControllers(PropertyQuery.PropertyController.class).forEach(controller ->
                controller.property(propertyQuery));
    }

    protected abstract Property createProperty(String key, Object value);

    @Override
    public void remove() {
        RemoveQuery<UniElement> removeQuery = new RemoveQuery<>(Arrays.asList(this), null);
        this.graph.getControllerManager().getControllers(RemoveQuery.RemoveController.class).forEach(controller ->
                controller.remove(removeQuery));
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public UniGraph getGraph() {
        return graph;
    }

    public static <E extends Element> Map<String, Object> fullProperties(E element) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(T.id.getAccessor(), element.id());
        properties.put(T.label.getAccessor(), element.label());
        element.properties().forEachRemaining(property -> properties.put(property.key(), property.value()));
        return properties;
    }
}
