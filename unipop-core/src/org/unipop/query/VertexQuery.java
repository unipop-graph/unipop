package org.unipop.query;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import java.util.List;

/**
 * A query with vertices
 */
public interface VertexQuery{
    /**
     * Returns query's vertices
     * @return List of vertices
     */
    List<Vertex> getVertices();

    /**
     * Returns query's direction
     * @return Direction
     */
    Direction getDirection();
}
