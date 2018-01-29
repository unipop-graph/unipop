package org.unipop.elastic.document;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.predicates.PredicatesHolder;
import org.elasticsearch.index.query.QueryBuilder;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.schema.element.VertexSchema;

import java.util.List;

/**
 * A schema that represents a vertex as a document in ES
 */
public interface DocumentVertexSchema extends DocumentSchema<Vertex>, VertexSchema {

    /**
     * Converts a Deferred vertex query to a query builder
     * @param query The deferred vertex query
     * @return A query builder
     */
    QueryBuilder getSearch(DeferredVertexQuery query);
    PredicatesHolder getVertexPredicates(List<Vertex> vertices);
}
