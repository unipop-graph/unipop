package org.unipop.controller;

import org.apache.tinkerpop.gremlin.structure.*;

import java.util.Map;

public interface VertexController extends ElementController<Vertex> {
    Vertex vertex(Object vertexId, String vertexLabel);
    Vertex addVertex(Object id, String label, Map<String, Object> properties);
}
