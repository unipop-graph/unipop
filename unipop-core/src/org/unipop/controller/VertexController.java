package org.unipop.controller;

import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.structure.BaseVertex;

import java.util.Iterator;

public interface VertexController {
    Iterator<BaseVertex> vertices(Object[] ids);
    Iterator<BaseVertex> vertices(Predicates predicates, MutableMetrics metrics);
    BaseVertex fromEdge(Direction direction, Object vertexId, String vertexLabel);
    BaseVertex addVertex(Object id, String label, Object[] properties);
}
