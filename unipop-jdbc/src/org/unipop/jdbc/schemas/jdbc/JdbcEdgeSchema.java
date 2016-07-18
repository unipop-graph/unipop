package org.unipop.jdbc.schemas.jdbc;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.schema.element.EdgeSchema;

import java.util.List;

/**
 * @author Gur Ronen
 * @since 3/7/2016
 */
public interface JdbcEdgeSchema extends JdbcSchema<Edge>, EdgeSchema {
    PredicatesHolder toPredicates(List<Vertex> vertices, Direction direction, PredicatesHolder predicates);
}
