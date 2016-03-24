package org.unipop.controller;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.structure.*;

import java.util.*;

public interface EdgeController extends Controller {
    Iterator<Edge>  edges(BaseVertex vertex, Direction direction, String[] edgeLabels);
    <E extends Element> Iterator<Traverser<E>>  edges(List<Traverser.Admin<Vertex>> vertices, Direction direction, String[] edgeLabels, Predicates predicates);
    Edge addEdge(Object edgeId, String label, BaseVertex outV, BaseVertex inV, Map<String, Object> properties);
}
