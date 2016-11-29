package org.unipop.rest.schema;

import com.mashape.unirest.request.BaseRequest;
import com.samskivert.mustache.Template;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.json.JSONObject;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.rest.RestVertexSchema;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.util.Map;

/**
 * Created by sbarzilay on 24/11/16.
 */
public class RestVertex extends AbstractRestSchema<Vertex> implements RestVertexSchema {
    public RestVertex(JSONObject configuration, String url, UniGraph graph, Template searchTemplate, Template searchUrlTemplate, Template addTemplate, Template addUrlTemplate, Template deleteUrlTemplate, String resultPath, JSONObject opTranslator, int maxResultSize) {
        super(configuration, graph, url, searchTemplate, searchUrlTemplate, addTemplate, addUrlTemplate, deleteUrlTemplate, resultPath, opTranslator, maxResultSize);
    }

    @Override
    public BaseRequest getSearch(DeferredVertexQuery query) {
        PredicatesHolder predicatesHolder = toPredicates(query.getVertices());
        return createSearch(predicatesHolder, query.getLimit());
    }

    @Override
    public Vertex createElement(Map<String, Object> fields) {
        Map<String, Object> properties = getProperties(fields);
        if (properties == null) return null;
        return new UniVertex(properties, graph);
    }

    @Override
    protected Vertex create(Map<String, Object> fields) {
        return createElement(fields);
    }
}
