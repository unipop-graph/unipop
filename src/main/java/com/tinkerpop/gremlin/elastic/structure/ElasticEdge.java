package com.tinkerpop.gremlin.elastic.structure;

import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.util.*;

import java.util.*;

public class ElasticEdge extends ElasticElement implements Edge, Edge.Iterators {

    public static String OutId = "outId";
    public static String InId = "inId";

    private String inId;
    private String outId;
    private ElasticService elasticService;

    public ElasticEdge(final Object id, final String label, Object outId, Object inId, Object[] keyValues, final ElasticGraph graph) {
        super(id, label, graph, keyValues);
        this.inId = inId.toString();
        this.outId = outId.toString();
        elasticService = graph.elasticService;
    }

    @Override
    public Property addPropertyLocal(String key, Object value) {
        if (!shouldAddProperty(key)) return Property.empty();
        ElasticProperty vertexProperty = new ElasticProperty(this, key, value);
        properties.put(key, vertexProperty);
        return vertexProperty;
    }

    @Override
    protected boolean shouldAddProperty(String key) {
        return super.shouldAddProperty(key) && key != OutId && key != InId;
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        checkRemoved();
        ElementHelper.validateProperty(key, value);
        ElasticProperty vertexProperty = (ElasticProperty) addPropertyLocal(key, value);
        elasticService.addProperty(this, key, value);
        return vertexProperty;
    }

    @Override
    public void remove() {
        checkRemoved();
        this.removed = true;
        elasticService.deleteElement(this);
    }

    @Override
    public String toString()
    {
        return "e[" + this.id() +"]["+this.outId+"-"+this.label +"->"+ this.inId+"]";
    }

    @Override
    public Edge.Iterators iterators() {
        checkRemoved();
        return this;
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction) {
        checkRemoved();
        return elasticService.getVertices(null,null,getVertexId(direction).toArray());
    }

    public List getVertexId(Direction direction) {
        switch (direction) {
            case OUT:
                return Arrays.asList(outId);
            case IN:
                return Arrays.asList(inId);
            default:
                return Arrays.asList(outId, inId);
        }
    }

    @Override
    public <V> Iterator<Property<V>> propertyIterator(final String... propertyKeys) {
        checkRemoved();
        return innerPropertyIterator(propertyKeys);
    }

    @Override
    protected void checkRemoved() {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Edge.class, this.id);
    }
}
