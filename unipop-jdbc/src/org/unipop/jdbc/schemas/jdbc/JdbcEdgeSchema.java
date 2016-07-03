package org.unipop.jdbc.schemas.jdbc;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.unipop.schema.element.EdgeSchema;

/**
 * @author Gur Ronen
 * @since 3/7/2016
 */
public interface JdbcEdgeSchema extends JdbcSchema<Edge>, EdgeSchema {
}
