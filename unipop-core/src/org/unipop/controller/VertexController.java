package org.unipop.controller;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.structure.BaseElement;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.BaseVertexProperty;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface VertexController extends Controller {
    BaseVertex vertex(Direction direction, Object vertexId, String vertexLabel);
    BaseVertex addVertex(Object id, String label, Map<String, Object> properties);
}
