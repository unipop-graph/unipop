package org.unipop.rest.schema;

import com.github.mustachejava.Mustache;
import com.mashape.unirest.request.BaseRequest;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.json.JSONObject;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.rest.RestVertexSchema;
import org.unipop.structure.UniGraph;

import java.util.Map;

/**
 * Created by sbarzilay on 24/11/16.
 */
public class RestVertex extends AbstractRestSchema<Vertex> implements RestVertexSchema {
    public RestVertex(JSONObject configuration, String url, UniGraph graph, Mustache searchTemplate, Mustache searchUrlTemplate, Mustache addTemplate, Mustache addUrlTemplate, Mustache deleteUrlTemplate) {
        super(configuration, graph, url, searchTemplate, searchUrlTemplate, addTemplate, addUrlTemplate, deleteUrlTemplate);
    }

    @Override
    public BaseRequest getSearch(DeferredVertexQuery query) {
        return null;
    }

    @Override
    public Vertex createElement(Map<String, Object> fields) {
        return null;
    }
}
