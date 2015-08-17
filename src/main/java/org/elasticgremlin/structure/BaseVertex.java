package org.elasticgremlin.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.*;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.elasticgremlin.queryhandler.Predicates;
import org.elasticsearch.action.get.MultiGetItemResponse;

import java.util.*;

public abstract class BaseVertex extends BaseElement implements Vertex {

    protected BaseVertex(Object id, String label, ElasticGraph graph, Object[] keyValues) {
        super(id, label, graph, keyValues);
    }

    @Override
    public Property createProperty(String key, Object value) {
        return new BaseVertexProperty(this, key, value);
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... propertyKeys) {
        checkRemoved();
        if(propertyKeys != null && propertyKeys.length > 0) throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        return this.property(key, value);
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        return edges(direction, edgeLabels, new Predicates());
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        return vertices(direction, edgeLabels, new Predicates());
    }

    public Iterator<Vertex> vertices(Direction direction, String[] edgeLabels, Predicates predicates) {
        checkRemoved();
        Iterator<Edge> edgeIterator = edges(direction, edgeLabels, predicates);
        ArrayList<Vertex> vertices = new ArrayList<>();
        if (edgeIterator != null) {
            edgeIterator.forEachRemaining(edge ->
                    vertices.add(vertexToVertex(this, edge, direction)));
        }
        return vertices.iterator();
    }

    public void applyLazyFields(MultiGetItemResponse response) {
        setLabel(response.getType());
        response.getResponse().getSource().entrySet().forEach((field) ->
                addPropertyLocal(field.getKey(), field.getValue()));
    }

    public static Vertex vertexToVertex(Vertex originalVertex, Edge edge, Direction direction) {
        switch (direction) {
            case OUT:
                return edge.inVertex();
            case IN:
                return edge.outVertex();
            case BOTH:
                Vertex outV = edge.outVertex();
                Vertex inV = edge.inVertex();
                if(outV.id().equals(inV.id()))
                    return originalVertex; //points to self
                if(originalVertex.id().equals(inV.id()))
                    return outV;
                if(originalVertex.id().equals(outV.id()))
                    return inV;
            default:
                throw new IllegalArgumentException(direction.toString());
        }
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
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        checkRemoved();
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);

        return graph.getQueryHandler().addEdge(idValue, label, this, vertex, keyValues);
    }

    @Override
    public void remove() {
        super.remove();
        Iterator<Edge> edges = edges(Direction.BOTH);
        edges.forEachRemaining(edge-> {
            edge.remove();
        });
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

    public Iterator<Edge> edges(Direction direction, String[] edgeLabels, Predicates predicates) {
        return graph.getQueryHandler().edges(IteratorUtils.asIterator(this), direction, edgeLabels, predicates);
    }
}