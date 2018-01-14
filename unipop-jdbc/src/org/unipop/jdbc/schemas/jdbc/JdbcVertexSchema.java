package org.unipop.jdbc.schemas.jdbc;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.schema.element.VertexSchema;

/**
 * A schema that represents a vertex as a JDBC row
 */
public interface JdbcVertexSchema extends JdbcSchema<Vertex>, VertexSchema {
}
