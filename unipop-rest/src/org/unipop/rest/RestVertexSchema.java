package org.unipop.rest;

import com.mashape.unirest.request.BaseRequest;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.schema.element.VertexSchema;

/**
 * A schema that represents a vertex in a rest data source.
 */
public interface RestVertexSchema extends RestSchema<Vertex>, VertexSchema{
    /**
     * Converts a Deferred vertex query to a HTTP request
     * @param query The deferred vertex query
     * @return A HTTP request
     */
    BaseRequest getSearch(DeferredVertexQuery query);
}
