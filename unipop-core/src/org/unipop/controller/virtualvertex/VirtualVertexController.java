package org.unipop.controller.virtualvertex;

import org.apache.commons.collections4.iterators.ArrayIterator;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.structure.BaseVertex;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.structure.*;

import java.util.*;

public class VirtualVertexController implements VertexController {
    private static final long VERTEX_BULK = 1000;

    private UniGraph graph;
    private String label;

    public VirtualVertexController(UniGraph graph, String label) {
        this.graph = graph;
        this.label = label;
    }

    @Override
    public Iterator<BaseVertex> vertices(Object[] vertexIds) {
        ArrayList<BaseVertex> vertices = new ArrayList<>();
        for(Object id : vertexIds) {
            BaseVertex vertex = new VirtualVertex(id, label, graph, null);
            vertices.add(vertex);
        }
        return new ArrayIterator<>(vertices.toArray());
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates, MutableMetrics metrics) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BaseVertex vertex(Direction direction, Object vertexId, String vertexLabel) {
        return new VirtualVertex(vertexId, vertexLabel, graph, null);
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Object[] properties) {
        throw new UnsupportedOperationException();
    }
}
