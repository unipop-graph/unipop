package org.unipop.jdbc.controller.edge;

import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.structure.BaseEdge;

import java.util.Iterator;

public class SqlEdgeController implements EdgeController {
    @Override
    public Iterator<BaseEdge> edges(Object[] ids) {
        return null;
    }

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates, MutableMetrics metrics) {
        return null;
    }

    @Override
    public Iterator<BaseEdge> fromVertex(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, MutableMetrics metrics) {
        return null;
    }

    @Override
    public BaseEdge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {
        return null;
    }
}
