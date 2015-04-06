package com.tinkerpop.gremlin.elastic.structure;

import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.util.*;

import java.util.*;

public class ElasticEdge extends ElasticElement implements Edge, Edge.Iterators {

    public static String OutId = "outId";
    public static String OutLabel = "outLabel";
    public static String InId = "inId";
    public static String InLabel = "inLabel";

    private String inId;
    private String outId;
    private String inLabel;
    private String outLabel;
    private ElasticService elasticService;

    public ElasticEdge(final Object id, final String label, Object outId,String outLabel, Object inId,String inLabel, Object[] keyValues, final ElasticGraph graph) {
        super(id, label, graph, keyValues);
        this.inId = inId.toString();
        this.inLabel = inLabel;
        this.outLabel = outLabel;
        this.outId = outId.toString();
        elasticService = graph.elasticService;
    }

    @Override
    public Property createProperty(String key, Object value) {
        return new ElasticProperty(this, key, value);
    }

    @Override
    protected boolean shouldAddProperty(String key) {
        return super.shouldAddProperty(key) && !(key.equals(OutId) || key.equals(OutLabel) || key.equals(InId) || key.equals(InLabel) ) ;
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
        elasticService.deleteElement(this);
        this.removed = true;
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
        ArrayList vertices = new ArrayList();
        //return elasticService.getVertices(null,null,getVertexId(direction).toArray());
        if(direction.equals(Direction.OUT) || direction.equals(Direction.BOTH))
            vertices.add(new ElasticVertex(outId,outLabel,null,graph,true));
        if(direction.equals(Direction.IN) || direction.equals(Direction.BOTH))
            vertices.add(new ElasticVertex(inId,inLabel,null,graph,true));
        return vertices.iterator();
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
