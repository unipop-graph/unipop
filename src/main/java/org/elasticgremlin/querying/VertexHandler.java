package org.elasticgremlin.querying;

import org.apache.tinkerpop.gremlin.structure.*;

import java.util.*;

public interface VertexHandler {
    Iterator<Vertex> vertices();
    Iterator<Vertex> vertices(Object[] vertexIds);
    public Iterator<Vertex> vertices(Predicates predicates);
    public Vertex vertex(Object vertexId, String vertexLabel, Edge edge, Direction direction);
    public Vertex addVertex(Object id, String label, Object[] properties);

}
