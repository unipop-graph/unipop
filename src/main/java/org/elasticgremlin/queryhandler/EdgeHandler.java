package org.elasticgremlin.queryhandler;

import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.structure.BaseVertex;

import java.util.*;

public interface EdgeHandler {
    Iterator<Edge> edges();
    Iterator<Edge> edges(Object[] edgeIds);
    public Iterator<Edge> edges(Predicates predicates);
    public Map<BaseVertex, List<Edge>> edges(Iterator<BaseVertex> vertex, Direction direction, String[] edgeLabels, Predicates predicates);
    public Edge addEdge(Object edgeId, String label,Vertex outV, Vertex inV, Object[] properties);
}
