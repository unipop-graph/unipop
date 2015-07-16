package org.elasticgremlin.queryhandler;

import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.structure.BaseVertex;

import java.util.Iterator;

public interface VertexHandler {
    Iterator<? extends Vertex> vertices();
    Iterator<? extends Vertex> vertices(Object[] vertexIds);
    public Iterator<? extends Vertex> vertices(Predicates predicates);
    public BaseVertex vertex(Object vertexId, String vertexLabel, Edge edge, Direction direction);
    public BaseVertex addVertex(Object id, String label, Object[] properties);

}
