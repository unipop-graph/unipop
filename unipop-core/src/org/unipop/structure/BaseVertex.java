package org.unipop.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.unipop.controller.EdgeController;
import org.unipop.controller.ElementController;
import org.unipop.controller.Predicates;
import org.unipop.common.util.StreamUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class BaseVertex extends BaseElement implements Vertex {

    public BaseVertex(Map<String, Object> keyValues, UniGraph graph) {
        super(keyValues,graph);
    }

    @Override
    public Property createProperty(String key, Object value) {
        return new BaseVertexProperty(this, key, value);
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateProperty(key, value);
        if(keyValues != null && keyValues.length > 0) throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        return this.property(key, value);
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        Predicates<Edge> edgePredicates = new Predicates<>(Edge.class, edgeLabels, null, null, 0);
        return graph.getControllerManager().getControllers(EdgeController.class).stream()
                .<Iterator<Edge>>map(controller -> controller.edges(Collections.singletonList(this), direction, edgePredicates))
                .flatMap(StreamUtils::asStream)
                .iterator();
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        return graph.traversal().V(this).toE(direction, edgeLabels).toV(direction.opposite());
    }

    @Override
    public <V> VertexProperty<V> property(String key, V value) {
        ElementHelper.validateProperty(key, value);
        BaseVertexProperty vertexProperty = (BaseVertexProperty) addPropertyLocal(key, value);
        graph.getControllerManager().getControllers(ElementController.class).forEach(controller -> controller.removeProperty(this, vertexProperty));
        return vertexProperty;
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        if (this.properties.containsKey(key)) {
            return (VertexProperty<V>) this.properties.get(key);
        }
        else return VertexProperty.<V>empty();
    }

    @Override
    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        if (null == vertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        Map<String, Object> stringObjectMap = UniGraph.asMap(keyValues);
        stringObjectMap.put(T.label.getAccessor(), label);
        return graph.getControllerManager().getControllers(EdgeController.class).stream()
                .map(controller -> controller.addEdge(this, (BaseVertex) vertex, stringObjectMap))
                .findFirst().get();
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
        return propertyIterator(propertyKeys);
    }

}