package org.elasticgremlin.queryhandler;

import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.structure.BaseVertex;

import java.util.*;

public interface EdgeHandler {
    Iterator<Edge> edges();
    Iterator<Edge> edges(Object[] edgeIds);
    public Iterator<Edge> edges(Predicates predicates);
    public Map<Object, List<Edge>> edges(Iterator<BaseVertex> vertices, Direction direction, String[] edgeLabels, Predicates predicates);
    public Edge addEdge(Object edgeId, String label,Vertex outV, Vertex inV, Object[] properties);
}
