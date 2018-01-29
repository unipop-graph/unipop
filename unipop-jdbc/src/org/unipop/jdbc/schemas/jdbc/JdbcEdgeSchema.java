package org.unipop.jdbc.schemas.jdbc;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.Select;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.schema.element.EdgeSchema;
import org.unipop.schema.element.VertexSchema;

import java.util.Collection;
import java.util.List;
import java.util.Map;

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
    Select getLocal(LocalQuery query);
    Collection<Pair<String, Element>> parseLocal(List<Map<String, Object>> result, LocalQuery query);
    VertexSchema getOutVertexSchema();
    VertexSchema getInVertexSchema();
}
