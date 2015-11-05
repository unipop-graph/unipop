package org.unipop.controller;

import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;

import java.util.*;

public interface EdgeController {
    Iterator<BaseEdge> edges(Predicates predicates, MutableMetrics metrics);
    Iterator<BaseEdge> fromVertex(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, MutableMetrics metrics);
    BaseEdge addEdge(Object edgeId, String label, BaseVertex outV, BaseVertex inV, Map<String, Object> properties);
}
