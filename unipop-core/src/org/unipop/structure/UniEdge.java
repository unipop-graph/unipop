package org.unipop.structure;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.unipop.query.mutation.PropertyQuery;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class UniEdge extends UniElement implements Edge {

    protected Map<String, Property> properties;
    protected Vertex inVertex;
    protected Vertex outVertex;

    public UniEdge(Map<String, Object> keyValues, Vertex outV, Vertex inV, final UniGraph graph) {
        super(keyValues, graph);

        this.outVertex = outV;
        this.inVertex = inV;
        this.properties = new HashMap<>();
        keyValues.forEach(this::addPropertyLocal);
    }

    @Override
    protected Map<String, Property> getPropertiesMap() {
        return properties;
    }

    @Override
    protected String getDefaultLabel() {
        return Edge.DEFAULT_LABEL;
    }

    @Override
    protected  Property createProperty(String key, Object value) {
        return new UniProperty<>(this, key, value);
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        UniProperty<V> vertexProperty = (UniProperty<V>) addPropertyLocal(key, value);
        PropertyQuery<UniElement> propertyQuery = new PropertyQuery<>(this, vertexProperty, PropertyQuery.Action.Add, null);
        this.graph.getControllerManager().getControllers(PropertyQuery.PropertyController.class).forEach(controller ->
                controller.property(propertyQuery));
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
