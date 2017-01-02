package org.unipop.rest;

import com.mashape.unirest.request.BaseRequest;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.schema.element.VertexSchema;

/**
 * Created by sbarzilay on 24/11/16.
 */
public interface RestVertexSchema extends RestSchema<Vertex>, VertexSchema{
    BaseRequest getSearch(DeferredVertexQuery query);
}
