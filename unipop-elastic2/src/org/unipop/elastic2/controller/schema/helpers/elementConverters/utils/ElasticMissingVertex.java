package org.unipop.elastic2.controller.schema.helpers.elementConverters.utils;

import org.apache.tinkerpop.gremlin.structure.*;

import java.util.Iterator;

/**
 * Created by Roman on 3/28/2015.
 */
public class ElasticMissingVertex implements Vertex {
    //region Static
    public static ElasticMissingVertex getInstance() {
        return instance;
    }

    private final static ElasticMissingVertex instance = new ElasticMissingVertex();
    //endregion

    //region Vertex Implementation
    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        throw Exceptions.edgeAdditionsNotSupported();
    }

    @Override
    public Object id() {
        return "missing";
    }

    @Override
    public String label() {
        return "";
    }

    @Override
    public Graph graph() {
        return null;
    }

    @Override
    public <V> VertexProperty<V> property(String key, V value) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String s, V v, Object... objects) {
        return null;
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... strings) {
        return null;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... strings) {
        return null;
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... strings) {
        return null;
    }

    @Override
    public void remove() {
        throw Exceptions.vertexRemovalNotSupported();
    }
}
