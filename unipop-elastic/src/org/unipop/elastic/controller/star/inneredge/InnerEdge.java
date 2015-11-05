package org.unipop.elastic.controller.star.inneredge;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.elastic.controller.star.ElasticStarVertex;
import org.unipop.structure.BaseEdge;

import java.util.Map;

public abstract class InnerEdge extends BaseEdge {

    private final InnerEdgeController mapping;

    public InnerEdge(ElasticStarVertex starVertex, Object edgeId, String edgeLabel, InnerEdgeController mapping, Vertex outVertex, Vertex inVertex, Map<String, Object> keyValues) {
        super(edgeId, edgeLabel, keyValues, outVertex, inVertex, starVertex.getController(), starVertex.getGraph());
        this.mapping = mapping;
    }

    public InnerEdgeController getInnerEdgeController() {
        return mapping;
    }
}
