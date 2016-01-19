package org.unipop.elastic2.controller.schema.helpers.elementConverters.utils;

import org.apache.tinkerpop.gremlin.structure.*;

import java.util.Iterator;

/**
 * Created by Roman on 3/28/2015.
 */
public class ElasticMissingEdge implements Edge {
    //region Static
    public static ElasticMissingEdge getInstance() {
        return instance;
    }

    private final static ElasticMissingEdge instance = new ElasticMissingEdge();
    //endregion

    //region Edge Implementation
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
    public <V> Property<V> property(String key, V value) {
        throw Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public void remove() {
        throw Exceptions.edgeRemovalNotSupported();
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        return null;
    }

    @Override
    public <V> Iterator<Property<V>> properties(String... strings) {
        return null;
    }
}
