package org.unipop.elastic.controller.edge;

import org.unipop.elastic.helpers.ElasticMutations;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.structure.*;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ElasticEdge extends BaseEdge {

    public static String OutId = "outId";
    public static String OutLabel = "outLabel";
    public static String InId = "inId";
    public static String InLabel = "inLabel";
    private final ElasticMutations elasticMutations;
    private final String indexName;


    public ElasticEdge(final Object id, final String label, Object[] keyValues, Vertex outV, Vertex inV, final UniGraph graph, ElasticMutations elasticMutations, String indexName) {
        super(id, label, keyValues, outV, inV, graph);
        this.elasticMutations = elasticMutations;
        this.indexName = indexName;
    }

    @Override
    protected void innerRemoveProperty(Property property) {
        elasticMutations.addElement(this, indexName, null, false);
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
    public Map<String, Object> allFields() {
        Map<String, Object> map = super.allFields();
        map.put(ElasticEdge.InId, inVertex.id());
        map.put(ElasticEdge.OutId, outVertex.id());
        map.put(ElasticEdge.InLabel, inVertex.label());
        map.put(ElasticEdge.OutLabel, outVertex.label());
        return map;
    }

    @Override
    protected void innerAddProperty(BaseProperty vertexProperty) {
        try {
            elasticMutations.updateElement(this, indexName, null, false);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
