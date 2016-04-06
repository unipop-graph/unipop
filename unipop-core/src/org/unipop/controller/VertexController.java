package org.unipop.controller;

import org.apache.tinkerpop.gremlin.structure.*;

import java.util.Map;

public interface VertexController extends ElementController {
    Vertex addVertex(Map<String, Object> properties);
}
