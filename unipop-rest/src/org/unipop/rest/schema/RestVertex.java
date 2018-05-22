package org.unipop.rest.schema;

import com.mashape.unirest.request.BaseRequest;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.rest.RestVertexSchema;
import org.unipop.rest.util.MatcherHolder;
import org.unipop.rest.util.TemplateHolder;
import org.unipop.schema.element.EdgeSchema;
import org.unipop.schema.element.ElementSchema;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.unipop.util.ConversionUtils.getList;

/**
 * Created by sbarzilay on 24/11/16.
 */
public class RestVertex extends AbstractRestSchema<Vertex> implements RestVertexSchema {
    private Set<ElementSchema> edgeSchemas = new HashSet<>();

    public RestVertex(JSONObject configuration, String url, UniGraph graph, TemplateHolder templateHolder, String resultPath, JSONObject opTranslator, int maxResultSize, MatcherHolder complexTranslator, boolean valuesToString) {
        super(configuration, graph, url, templateHolder, resultPath, opTranslator, maxResultSize, complexTranslator, valuesToString);
        for (JSONObject edgeJson : getList(json, "edges")){
            EdgeSchema docEdgeSchema = getEdgeSchema(edgeJson);
            edgeSchemas.add(docEdgeSchema);
        }
    }

    RestVertex(JSONObject configuration, String url, String resource, UniGraph graph, TemplateHolder templateHolder, String resultPath, JSONObject opTranslator, int maxResultSize, MatcherHolder complexTranslator, boolean valuesToString) {
        super(configuration, graph, url, resource, templateHolder, resultPath, opTranslator, maxResultSize, complexTranslator, valuesToString);
        for (JSONObject edgeJson : getList(json, "edges")){
            EdgeSchema docEdgeSchema = getEdgeSchema(edgeJson);
            edgeSchemas.add(docEdgeSchema);
        }
    }

    private EdgeSchema getEdgeSchema(JSONObject edgeJson) throws JSONException {
        Direction direction = Direction.valueOf(edgeJson.optString("direction"));
        return new InnerEdgeRestSchema(edgeJson, this.graph, this.baseUrl, this.templateHolder, this.resultPath, this.opTranslator, this.maxResultSize, this.complexTranslator, this.valuesToString, direction, this, this.resource);
    }

    @Override
    public BaseRequest getSearch(DeferredVertexQuery query) {
        int limit = query.getOrders() == null || query.getOrders().size() > 0 ? -1 : query.getLimit();
        PredicatesHolder predicatesHolder = toPredicates(query.getVertices());
        return createSearch(predicatesHolder, limit);
    }

    @Override
    public Vertex createElement(Map<String, Object> fields) {
        Map<String, Object> properties = getProperties(fields);
        if (properties == null) return null;
        return new UniVertex(properties, this, graph);
    }

    public Set<ElementSchema> getChildSchemas(){
        return this.edgeSchemas;
    }

    public String getResource(){
        return this.resource;
    }

    @Override
    protected Vertex create(Map<String, Object> fields) {
        return createElement(fields);
    }
}
