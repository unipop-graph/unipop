package org.unipop.elastic.document;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.elasticsearch.index.query.QueryBuilder;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.element.EdgeSchema;

/**
 * A schema that represents an edge as a document in ES
 */
public interface DocumentEdgeSchema extends DocumentSchema<Edge>, EdgeSchema {

    /**
     * Converts a Search vertex query to a query builder
     * @param query The search vertex query
     * @return A query builder
     */
    QueryBuilder getSearch(SearchVertexQuery query);
}
