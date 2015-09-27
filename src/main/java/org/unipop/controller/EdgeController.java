package org.unipop.controller;

import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.*;

public interface EdgeController {
    Iterator<Edge> edges(Object[] ids);
    Iterator<Edge> edges(Predicates predicates, MutableMetrics metrics);
    Iterator<Edge> edges(Iterator<Vertex> vertices, Direction direction, String[] edgeLabels, Predicates predicates);
    Edge addEdge(Object edgeId, String label,Vertex outV, Vertex inV, Object[] properties);

}
