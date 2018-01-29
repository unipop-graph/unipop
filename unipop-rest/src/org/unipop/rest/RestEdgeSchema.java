package org.unipop.rest;

import com.mashape.unirest.request.BaseRequest;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.element.EdgeSchema;

/**
 * A schema that represents an edge in a rest data source.
 */
public interface RestEdgeSchema extends RestSchema<Edge>, EdgeSchema{

    /**
     * Converts a Search vertex query to a HTTP request
     * @param query The search vertex query
     * @return A HTTP request
     */
    BaseRequest getSearch(SearchVertexQuery query);
}
