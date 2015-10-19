package org.unipop.jdbc.controller.vertex;

import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.structure.BaseVertex;

import java.util.Iterator;

public class SqlVertexController implements VertexController {
    @Override
    public Iterator<BaseVertex> vertices(Object[] ids) {
        return null;
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates, MutableMetrics metrics) {
        return null;
    }

    @Override
    public BaseVertex fromEdge(Direction direction, Object vertexId, String vertexLabel) {
        return null;
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Object[] properties) {
        return null;
    }
}
