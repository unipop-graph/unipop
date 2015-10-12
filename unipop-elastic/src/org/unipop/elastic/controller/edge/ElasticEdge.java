package org.unipop.elastic.controller.edge;

import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.collect.Iterators2;
import org.elasticsearch.common.collect.Lists;
import org.unipop.elastic.helpers.ElasticMutations;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.structure.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ElasticEdge extends BaseEdge {

    public static String OutId = "outId";
    public static String OutLabel = "outLabel";
    public static String InId = "inId";
    public static String InLabel = "inLabel";
    private final ElasticMutations elasticMutations;
    private final String indexName;
    protected Vertex inVertex;
    protected Vertex outVertex;

    public ElasticEdge(String id, String label, Object[] properties, Object outVid, String outVlabel, Object inVid, String inVlabel, UniGraph graph, ElasticMutations elasticMutations, String indexName) {
        super(id, label, properties, graph);
        this.elasticMutations = elasticMutations;
        this.indexName = indexName;

        this.outVertex = graph.getControllerProvider().vertex(this, Direction.OUT, outVid, outVlabel);
        this.inVertex = graph.getControllerProvider().vertex(this, Direction.IN, inVid, inVlabel);
    }

    public ElasticEdge(Object edgeId, String label, Object[] properties, Vertex outV, Vertex inV, UniGraph graph, ElasticMutations elasticMutations, String indexName) {
        super(edgeId, label, properties, graph);

        this.elasticMutations = elasticMutations;
        this.indexName = indexName;

        this.outVertex = outV;
        this.inVertex = inV;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        checkRemoved();
        if(direction.equals(Direction.OUT)) return Iterators.singletonIterator(outVertex);
        if(direction.equals(Direction.IN)) return Iterators.singletonIterator(inVertex);
        return Lists.newArrayList(outVertex, inVertex).iterator();
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
