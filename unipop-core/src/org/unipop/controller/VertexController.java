package org.unipop.controller;

import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.structure.BaseVertex;

import java.util.Iterator;

public interface VertexController {
    Iterator<Vertex> vertices(Object[] ids);
    Iterator<? extends Vertex> vertices(Predicates predicates, MutableMetrics metrics);
    BaseVertex vertex(Object vertexId, String vertexLabel, Direction direction);
    BaseVertex addVertex(Object id, String label, Object[] properties);
}
