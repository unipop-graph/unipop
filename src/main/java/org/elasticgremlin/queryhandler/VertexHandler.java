package org.elasticgremlin.queryhandler;

import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.structure.BaseVertex;

import java.util.Iterator;

public interface VertexHandler {
    Iterator<Vertex> vertices();
    Iterator<Vertex> vertices(Object[] vertexIds);
    public Iterator<BaseVertex> vertices(Predicates predicates);
    public BaseVertex vertex(Object vertexId, String vertexLabel, Edge edge, Direction direction);
    public BaseVertex addVertex(Object id, String label, Object[] properties);

}
