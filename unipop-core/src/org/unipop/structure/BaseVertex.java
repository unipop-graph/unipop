package org.unipop.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.unipop.controller.VertexController;

import java.util.Iterator;
import java.util.Map;

public abstract class BaseVertex<C extends VertexController> extends BaseElement implements Vertex {

    private C controller;

    protected BaseVertex(Object id, String label, Map<String, Object> keyValues, C controller, UniGraph graph) {
        super(id, label, graph, keyValues);
        this.controller = controller;
    }

    @Override
    public Property createProperty(String key, Object value) {
        return new BaseVertexProperty(this, key, value);
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, final Object... keyValues) {
        checkRemoved();
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateProperty(key, value);
        if(keyValues != null && keyValues.length > 0) throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        return this.property(key, value);
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        return graph.traversal().V(this).toE(direction, edgeLabels);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        return graph.traversal().V(this).toE(direction, edgeLabels).toV(direction.opposite());
    }

    public void applyLazyFields(String label, Map<String, Object> properties) {
        setLabel(label);
        properties.entrySet().forEach((field) ->
                addPropertyLocal(field.getKey(), field.getValue()));
    }

    @Override
    public <V> VertexProperty<V> property(String key, V value) {
        checkRemoved();
        ElementHelper.validateProperty(key, value);
        BaseVertexProperty vertexProperty = (BaseVertexProperty) addPropertyLocal(key, value);
        innerAddProperty(vertexProperty);
        return vertexProperty;
    }

    protected abstract void innerAddProperty(BaseVertexProperty vertexProperty);

    @Override
    public <V> VertexProperty<V> property(final String key) {
        checkRemoved();
        if (this.properties.containsKey(key)) {
            return (VertexProperty<V>) this.properties.get(key);
        }
        else return VertexProperty.<V>empty();
    }

    @Override
    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        if (null == vertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        checkRemoved();
        Map<String, Object> stringObjectMap = UniGraph.asMap(keyValues);
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        stringObjectMap.remove("id");
        stringObjectMap.remove("label");
        BaseEdge edge = graph.getControllerManager().addEdge(idValue, label, this, (BaseVertex) vertex, stringObjectMap);
        return edge;
    }

    @Override
    public void remove() {
        Iterator<Edge> edges = edges(Direction.BOTH);
        edges.forEachRemaining(edge-> {
            edge.remove();
        });
        super.remove();
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        checkRemoved();
        return innerPropertyIterator(propertyKeys);
    }

    @Override
    protected void checkRemoved() {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id);
    }

    public C getController() {
        return controller;
    }
}