package org.unipop.rest.schema;

//import com.github.Templatejava.DefaultTemplateFactory;
//import com.github.Templatejava.Template;
//import com.github.Templatejava.TemplateFactory;

import com.samskivert.mustache.Template;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.request.BaseRequest;
import com.mashape.unirest.request.body.RequestBodyEntity;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.json.JSONArray;
import org.json.JSONObject;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.search.SearchQuery;
import org.unipop.rest.RestSchema;
import org.unipop.rest.util.PredicatesTranslator;
import org.unipop.schema.element.AbstractElementSchema;
import org.unipop.schema.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 24/11/16.
 */
public abstract class AbstractRestSchema<E extends Element> extends AbstractElementSchema<E> implements RestSchema<E> {

    protected String baseUrl;
    protected String resource;
    protected Template searchUrlTemplate;
    protected Template searchTemplate;
    protected Template addTemplate;
    protected Template addUrlTemplate;
    protected Template deleteUrlTemplate;
    protected String resultPath;
    protected JSONObject opTranslator;
    protected int maxResultSize;

    public AbstractRestSchema(JSONObject configuration, UniGraph graph, String url, Template searchTemplate, Template searchUrlTemplate, Template addTemplate, Template addUrlTemplate, Template deleteUrlTemplate, String resultPath, JSONObject opTranslator, int maxResultSize) {
        super(configuration, graph);
        this.resource = configuration.optString("resource");
        this.searchTemplate = searchTemplate;
        this.searchUrlTemplate = searchUrlTemplate;
        this.addTemplate = addTemplate;
        this.addUrlTemplate = addUrlTemplate;
        this.deleteUrlTemplate = deleteUrlTemplate;
        this.baseUrl = url;
        this.resultPath = resultPath;
        this.opTranslator = opTranslator;
        this.maxResultSize = maxResultSize;
    }

    @Override
    public BaseRequest getSearch(SearchQuery<E> query) {
        return createSearch(this.toPredicates(query.getPredicates()), query.getLimit());
    }

    protected BaseRequest createSearch(PredicatesHolder predicatesHolder, int limit){
        if (limit == -1)
            limit = maxResultSize;
        else
            limit = Math.min(maxResultSize, limit);
        Map<String, Object> predicates = PredicatesTranslator.translate(predicatesHolder, opTranslator, limit);

        String body = searchTemplate.execute(predicates);
        Map<String, Object> urlMap = new HashMap<>();
        urlMap.put("resource", resource);
        String url = searchUrlTemplate.execute(urlMap);
        RequestBodyEntity request = Unirest.post(baseUrl + url.toString())
                .body(body.toString());

        return request;
    }

    protected Set<String> toFields(){
        Set<String> keys = propertySchemas.stream().map(PropertySchema::getKey).collect(Collectors.toSet());
        return toFields(keys);
    }

    @Override
    public List<E> parseResults(HttpResponse<JsonNode> result, PredicateQuery query) {
        String[] path = resultPath.split("\\.");
        JSONObject object = result.getBody().getObject();
        JSONArray results = null;
        for (int i = 0; i < path.length; i++) {
            if (i == path.length - 1)
                results = object.getJSONArray(path[i]);
            else
                object = object.getJSONObject(path[i]);
        }
        List<E> elements = new ArrayList<>();
        for (int i = 0; i < results.length(); i++) {
            JSONObject jsonResult = results.getJSONObject(i);

            Set<String> fields = toFields();
            Map<String, Object> fieldsMap = new HashMap<>();
            fields.forEach(field -> {
                String[] fieldPath = field.split("\\.");
                Object data = jsonResult;
                for (String key : fieldPath) {
                    if (data instanceof JSONObject)
                        data = ((JSONObject) data).opt(key);
                }
                if (data != null) {
                    if (!NumberUtils.isNumber(data.toString()))
                        fieldsMap.put(field, data);
                    else
                        fieldsMap.put(field, NumberUtils.createNumber(data.toString()));
                }
            });
            E element = create(fieldsMap);
            if(element != null && query.test(element, query.getPredicates()))
                elements.add(element);
        }
        return elements;
    }

    protected abstract E create(Map<String, Object> fields);

    @Override
    public BaseRequest addElement(E element) {
        Map<String, Object> stringObjectMap = toFields(element)
                .entrySet().stream()
                .map(entry -> {
                    String[] split = entry.getKey().split("\\.");
                    return new HashMap.SimpleEntry<>(split[split.length - 1], entry.getValue());
                }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<String, Object> urlMap = new HashMap<>();
        urlMap.putAll(stringObjectMap);
        urlMap.put("resource", resource);
        String url = addUrlTemplate.execute(urlMap);

        String body = addTemplate.execute(Collections.singletonMap("prop", stringObjectMap.entrySet()));

        return Unirest.post(baseUrl + url.toString()).body(body);
    }

    @Override
    public BaseRequest delete(E element) {
//        StringWriter url = new StringWriter();
        String url = deleteUrlTemplate.execute(element);
        return Unirest.delete(baseUrl + url.toString());
    }
}
