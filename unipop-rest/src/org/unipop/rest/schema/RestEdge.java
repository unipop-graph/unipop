package org.unipop.rest.schema;

import com.mashape.unirest.http.Unirest;
import com.samskivert.mustache.Template;
import com.mashape.unirest.request.BaseRequest;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.rest.RestEdgeSchema;
import org.unipop.rest.util.TemplateHolder;
import org.unipop.schema.element.VertexSchema;
import org.unipop.schema.reference.ReferenceVertexSchema;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by sbarzilay on 27/11/16.
 */
public class RestEdge extends AbstractRestSchema<Edge> implements RestEdgeSchema {
    protected VertexSchema outVertexSchema;
    protected VertexSchema inVertexSchema;

    public RestEdge(JSONObject configuration, UniGraph graph, String url, TemplateHolder templateHolder, String resultPath, JSONObject opTranslator, int maxResultSize) {
        super(configuration, graph, url, templateHolder, resultPath, opTranslator, maxResultSize);
        this.outVertexSchema = createVertexSchema("outVertex");
        this.inVertexSchema = createVertexSchema("inVertex");
    }

    protected VertexSchema createVertexSchema(String key) throws JSONException {
        JSONObject vertexConfiguration = this.json.optJSONObject(key);
        if (vertexConfiguration == null) return null;
        if (vertexConfiguration.optBoolean("ref", false)) return new ReferenceVertexSchema(vertexConfiguration, graph);
        return new RestVertex(vertexConfiguration, baseUrl, graph, templateHolder, resultPath, opTranslator, maxResultSize);
    }


    @Override
    protected Edge create(Map<String, Object> fields) {
        return new UniEdge(getProperties(fields), outVertexSchema.createElement(fields), inVertexSchema.createElement(fields), graph);
    }

    @Override
    public Collection<Edge> fromFields(Map<String, Object> fields) {
        return Collections.singleton(create(fields));
    }

    @Override
    public BaseRequest getSearch(SearchVertexQuery query) {
        int limit = query.getOrders() == null || query.getOrders().size() > 0 ? -1 : query.getLimit();
        PredicatesHolder edgePredicates = this.toPredicates(query.getPredicates());
        PredicatesHolder vertexPredicates = this.getVertexPredicates(query.getVertices(), query.getDirection());
        PredicatesHolder predicatesHolder = PredicatesHolderFactory.and(edgePredicates, vertexPredicates);
        return createSearch(predicatesHolder, limit);
    }

    @Override
    protected Set<String> toFields() {
        Set<String> edgeFields = super.toFields();
        Set<String> outVertexFields = outVertexSchema instanceof RestVertex ?
                ((RestVertex) outVertexSchema).toFields() :
                outVertexSchema.toFields(Collections.emptySet());
        Set<String> inVertexFields = inVertexSchema instanceof RestVertex ?
                ((RestVertex) inVertexSchema).toFields() :
                inVertexSchema.toFields(Collections.emptySet());
        return Stream.of(edgeFields, outVertexFields, inVertexFields).flatMap(Collection::stream).collect(Collectors.toSet());
    }

    @Override
    public BaseRequest addElement(Edge element) throws NoSuchElementException{
        Map<String, Object> outVertexFields = outVertexSchema.toFields(element.outVertex());
        Map<String, Object> inVertexFields = inVertexSchema.toFields(element.inVertex());
        Map<String, Object> fields = toFields(element);
        if (fields == null || outVertexFields == null || inVertexFields == null) throw new NoSuchElementException();
        Map<String, Object> stringObjectMap = Stream.of(outVertexFields.entrySet(),
                inVertexFields.entrySet(),
                fields.entrySet()).flatMap(Collection::stream)
                .map(entry -> {
                    String[] split = entry.getKey().split("\\.");
                    return new HashMap.SimpleEntry<>(split[split.length - 1], entry.getValue());
                }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<String, Object> urlMap = new HashMap<>();
        urlMap.putAll(stringObjectMap);
        urlMap.put("resource", resource);
        return insertElement(urlMap, stringObjectMap);
    }

    protected PredicatesHolder getVertexPredicates(List<Vertex> vertices, Direction direction) {
        PredicatesHolder outPredicates = this.outVertexSchema.toPredicates(vertices);
        PredicatesHolder inPredicates = this.inVertexSchema.toPredicates(vertices);
        if (direction.equals(Direction.OUT) && outPredicates.notAborted()) return outPredicates;
        if (direction.equals(Direction.IN) && inPredicates.notAborted()) return inPredicates;
        if (outPredicates.notAborted() && inPredicates.notAborted())
            return PredicatesHolderFactory.or(inPredicates, outPredicates);
        else if (outPredicates.isAborted()) return inPredicates;
        else if (inPredicates.isAborted()) return outPredicates;
        else return PredicatesHolderFactory.abort();
    }
}
