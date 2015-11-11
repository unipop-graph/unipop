package org.unipop.controller;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.structure.BaseVertex;

import java.util.Iterator;
import java.util.Map;

public interface VertexController {
    Iterator<BaseVertex> vertices(Predicates predicates, Metrics metrics);
    BaseVertex vertex(Direction direction, Object vertexId, String vertexLabel);

    long vertexCount(Predicates predicates);

    Map<String, Object> vertexGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal);

    BaseVertex addVertex(Object id, String label, Map<String, Object> properties);
}
