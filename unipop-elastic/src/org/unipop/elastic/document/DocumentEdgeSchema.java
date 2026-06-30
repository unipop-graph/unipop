package org.unipop.elastic.document;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.element.EdgeSchema;

/**
 * A schema that represents an edge as a document in ES
 */
public interface DocumentEdgeSchema extends DocumentSchema<Edge>, EdgeSchema {

    /**
     * Converts a Search vertex query to a Query
     * @param query The search vertex query
     * @return A Query
     */
    Query getSearch(SearchVertexQuery query);
}
