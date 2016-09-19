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

/**
 * @author Gur Ronen
 * @since 3/7/2016
 */
public interface JdbcEdgeSchema extends JdbcSchema<Edge>, EdgeSchema {
    PredicatesHolder toPredicates(List<Vertex> vertices, Direction direction, PredicatesHolder predicates);
    Select getLocal(LocalQuery query, DSLContext dsl);
    Collection<Pair<String, Element>> parseLocal(Result result, LocalQuery query);
    VertexSchema getOutVertexSchema();
    VertexSchema getInVertexSchema();
}
