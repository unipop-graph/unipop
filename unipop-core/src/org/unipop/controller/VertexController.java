package org.unipop.controller;

import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.structure.BaseVertex;

import java.util.Iterator;

public interface VertexController {
    Iterator<Vertex> vertices(Object[] ids);
    Iterator<Vertex> vertices(Predicates predicates, MutableMetrics metrics);
    BaseVertex vertex(Edge edge, Direction direction, Object vertexId, String vertexLabel);
    BaseVertex addVertex(Object id, String label, Object[] properties);
}
