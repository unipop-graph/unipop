package org.unipop.jdbc.schemas.jdbc;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.schema.element.VertexSchema;

/**
 * @author Gur Ronen
 * @since 3/7/2016
 */
public interface JdbcVertexSchema extends JdbcSchema<Vertex>, VertexSchema {
}
