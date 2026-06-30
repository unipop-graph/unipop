package org.unipop.elastic.document;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.schema.element.VertexSchema;

/**
 * A schema that represents a vertex as a document in ES
 */
public interface DocumentVertexSchema extends DocumentSchema<Vertex>, VertexSchema {

    /**
     * Converts a Deferred vertex query to a Query
     * @param query The deferred vertex query
     * @return A Query
     */
    Query getSearch(DeferredVertexQuery query);
}
