package org.unipop.query;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import java.util.List;

public interface VertexQuery{
    List<Vertex> getVertices();
    Direction getDirection();
}
