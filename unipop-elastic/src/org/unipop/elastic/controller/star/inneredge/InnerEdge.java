package org.unipop.elastic.controller.star.inneredge;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.elastic.controller.star.ElasticStarVertex;
import org.unipop.structure.BaseEdge;

import java.util.Map;

public abstract class InnerEdge<T extends InnerEdgeController> extends BaseEdge {

    private final T innerEdgeController;

    public InnerEdge(ElasticStarVertex starVertex, Object edgeId, String edgeLabel, T innerEdgeController, Vertex outVertex, Vertex inVertex) {
        super(edgeId, edgeLabel, null, outVertex, inVertex, starVertex.getController(), starVertex.getGraph());
        this.innerEdgeController = innerEdgeController;
    }

    public T getInnerEdgeController() {
        return innerEdgeController;
    }
}
