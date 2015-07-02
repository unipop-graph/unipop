package org.elasticgremlin.queryhandler.elasticsearch.edgedoc;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.elasticgremlin.queryhandler.elasticsearch.helpers.ElasticMutations;
import org.elasticgremlin.structure.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class DocEdge extends BaseEdge {

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

    public DocEdge(final Object id, final String label, Object outId, String outLabel, Object inId, String inLabel, Object[] keyValues, final ElasticGraph graph, ElasticMutations elasticMutations, String indexName) {
        super(id, label, keyValues, graph);
        this.elasticMutations = elasticMutations;
        this.indexName = indexName;
        ElementHelper.validateLabel(outLabel);
        ElementHelper.validateLabel(inLabel);

        this.inId = inId.toString();
        this.inLabel = inLabel;
        this.outLabel = outLabel;
        this.outId = outId.toString();

        this.inVertex = graph.getQueryHandler().vertex(this.inId, this.inLabel, this, Direction.IN);
        this.outVertex = graph.getQueryHandler().vertex(this.outId, this.outLabel, this, Direction.OUT);
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
        map.put(DocEdge.InId, inId);
        map.put(DocEdge.OutId, outId);
        map.put(DocEdge.InLabel, inLabel);
        map.put(DocEdge.OutLabel, outLabel);
        return map;
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
