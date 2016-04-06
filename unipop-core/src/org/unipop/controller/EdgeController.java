package org.unipop.controller;

import org.apache.tinkerpop.gremlin.structure.*;

import java.util.*;

public interface EdgeController extends ElementController {
    Iterator<Edge> edges(List<Vertex> vertices, Direction direction, Predicates<Edge> predicates);
    Edge addEdge(Vertex outV, Vertex inV, Map<String, Object> properties);
}
