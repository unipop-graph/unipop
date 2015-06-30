package org.elasticgremlin.elasticsearch.edge;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.elasticgremlin.elasticsearch.ElasticMutations;
import org.elasticgremlin.structure.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class ElasticDocEdge extends BaseEdge {

    public static String OutId = "outId";
    public static String OutLabel = "outLabel";
    public static String InId = "inId";
    public static String InLabel = "inLabel";
    private final ElasticMutations elasticMutations;
    private final String indexName;

    public String inId;
    public String outId;
    public String inLabel;
    public String outLabel;

    public ElasticDocEdge(final Object id, final String label, Object outId, String outLabel, Object inId, String inLabel, Object[] keyValues, final ElasticGraph graph, ElasticMutations elasticMutations, String indexName) {
        super(id, label, keyValues, graph);
        this.elasticMutations = elasticMutations;
        this.indexName = indexName;
        ElementHelper.validateLabel(outLabel);
        ElementHelper.validateLabel(inLabel);

        this.inId = inId.toString();
        this.inLabel = inLabel;
        this.outLabel = outLabel;
        this.outId = outId.toString();
    }

    @Override
    protected void innerRemoveProperty(Property property) {
        try {
            elasticMutations.updateElement(this, indexName, null, false);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected boolean shouldAddProperty(String key) {
        return super.shouldAddProperty(key) && !key.equals(OutId) && !key.equals(OutLabel) && !key.equals(InId) && !key.equals(InLabel);
    }

    @Override
    protected void innerRemove() {
        elasticMutations.deleteElement(this, indexName, null);
    }

    @Override
    public String toString()
    {
        return "e[" + this.id() +"]["+this.outId+"-"+this.label +"->"+ this.inId+"]";
    }

    @Override
    public Map<String, Object> allFields() {
        Map<String, Object> map = super.allFields();
        map.put(ElasticDocEdge.InId, inId);
        map.put(ElasticDocEdge.OutId, outId);
        map.put(ElasticDocEdge.InLabel, inLabel);
        map.put(ElasticDocEdge.OutLabel, outLabel);
        return map;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        checkRemoved();
        ArrayList vertices = new ArrayList();
        if(direction.equals(Direction.OUT) || direction.equals(Direction.BOTH))
            vertices.add(graph.getQueryHandler().vertex(outId, outLabel, this, direction));
        if(direction.equals(Direction.IN) || direction.equals(Direction.BOTH))
            vertices.add(graph.getQueryHandler().vertex(inId, inLabel, this, direction));
        return vertices.iterator();
    }

    @Override
    protected void innerAddProperty(BaseProperty vertexProperty) {
        try {
            elasticMutations.updateElement(this, indexName, null, false);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
