package org.unipop.controller.virtualvertex;

import com.fasterxml.jackson.databind.util.ArrayIterator;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.controller.*;
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
    public Iterator<Vertex> vertices(Object[] vertexIds) {
        ArrayList<BaseVertex> vertices = new ArrayList<>();
        for(Object id : vertexIds) {
            BaseVertex vertex = new VirtualVertex(id, label, graph, null);
            vertices.add(vertex);
        }
        return new ArrayIterator<>((Vertex[]) vertices.toArray()).iterator();
    }

    @Override
    public Iterator<Vertex> vertices(Predicates predicates, MutableMetrics metrics) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BaseVertex vertex(Object vertexId, String vertexLabel, Direction direction) {
        BaseVertex vertex = new VirtualVertex(vertexId, vertexLabel, graph, null);
        return vertex;
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Object[] properties) {
        throw new UnsupportedOperationException();
    }
}
