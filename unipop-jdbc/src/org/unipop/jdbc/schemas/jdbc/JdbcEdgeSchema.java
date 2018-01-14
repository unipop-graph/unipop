package org.unipop.jdbc.schemas.jdbc;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.schema.element.EdgeSchema;

import java.util.List;

/**
 * A schema that represents an edge as a JDBC row
 */
public interface JdbcEdgeSchema extends JdbcSchema<Edge>, EdgeSchema {

    /**
     * Converts a list of vertices to a predicates holder
     * @param vertices The vertices
     * @param direction The direction of the query
     * @param predicates The predicates of the query
     * @return A predicates holder
     */
    PredicatesHolder toPredicates(List<Vertex> vertices, Direction direction, PredicatesHolder predicates);
}
