package org.unipop.rest;

import com.mashape.unirest.request.BaseRequest;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.element.EdgeSchema;

/**
 * Created by sbarzilay on 24/11/16.
 */
public interface RestEdgeSchema extends RestSchema<Edge>, EdgeSchema{
    BaseRequest getSearch(SearchVertexQuery query);
}
