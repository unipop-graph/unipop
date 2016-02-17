package org.unipop.jdbc.controller.star.inneredge;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.controller.EdgeController;
import org.unipop.jdbc.controller.star.SqlStarVertex;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.UniGraph;

import java.util.Map;

/**
 * Created by sbarzilay on 2/17/16.
 */
public abstract class InnerEdge <T extends InnerEdgeController> extends BaseEdge {
    private final T innerEdgeController;

    public InnerEdge(SqlStarVertex starVertex,Object id, String label, Vertex outV, Vertex inV, T controller) {
        super(id, label, null, outV, inV, starVertex.getController(), starVertex.getGraph());
        innerEdgeController = controller;
    }

    public T getInnerEdgeController() {
        return innerEdgeController;
    }
}
