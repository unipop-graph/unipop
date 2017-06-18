package org.unipop.rest.schema;

import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.request.BaseRequest;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.json.JSONArray;
import org.json.JSONObject;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.search.SearchQuery;
import org.unipop.rest.RestSchema;
import org.unipop.rest.util.MatcherHolder;
import org.unipop.rest.util.PredicatesTranslator;
import org.unipop.rest.util.TemplateHolder;
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
    protected TemplateHolder templateHolder;
    protected String resultPath;
    protected JSONObject opTranslator;
    protected int maxResultSize;
    protected List<Map<String, Object>> bulk;
    protected int bulkSize;
    protected MatcherHolder complexTranslator;
    protected boolean valuesToString;

    public AbstractRestSchema(JSONObject configuration, UniGraph graph, String url, TemplateHolder templateHolder, String resultPath, JSONObject opTranslator, int maxResultSize, MatcherHolder complexTranslator, boolean valuesToString) {
        super(configuration, graph);
        this.resource = configuration.optString("resource");
        this.templateHolder = templateHolder;
        this.baseUrl = url;
        this.resultPath = resultPath;
        this.opTranslator = opTranslator;
        this.maxResultSize = maxResultSize;
        this.bulk = new ArrayList<>();
        this.bulkSize = 1000;
        this.complexTranslator = complexTranslator;
        this.valuesToString = valuesToString;
    }

    @Override
    public BaseRequest getSearch(SearchQuery<E> query) {
        int limit = query.getOrders() == null || query.getOrders().size() > 0 ? -1 : query.getLimit();
        return createSearch(this.toPredicates(query.getPredicates()), limit);
    }

    protected BaseRequest createSearch(PredicatesHolder predicatesHolder, int limit) {
        if (predicatesHolder.isAborted())
            return null;
        if (limit == -1)
            limit = maxResultSize;
        else
            limit = Math.min(maxResultSize, limit);
        Map<String, Object> predicates = PredicatesTranslator.translate(predicatesHolder, opTranslator, complexTranslator, valuesToString, limit);

        Map<String, Object> urlMap = new HashMap<>();
        urlMap.put("resource", resource);
        predicates.put("resource", resource);
        BaseRequest request = templateHolder.getSearch().execute(baseUrl, urlMap, predicates);

        try {
            if (bulk.size() > 0)
                runBulk();
            if (templateHolder.isCommit())
                templateHolder.getCommit().execute(baseUrl, urlMap, predicates).asJson();
        } catch (UnirestException e) {
            e.printStackTrace();
        }

        return request;
    }

    protected Set<String> toFields() {
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
                    else {
                        Number number = NumberUtils.createNumber(data.toString());
                        if (number instanceof Float)
                            number = Double.parseDouble(data.toString());
                        fieldsMap.put(field, number);
                    }
                }
            });
            E element = create(fieldsMap);
            if (element != null && query.test(element, query.getPredicates()))
                elements.add(element);
        }
        return elements;
    }

    protected abstract E create(Map<String, Object> fields);

    private void runBulk(){
        BaseRequest bulk = templateHolder.getBulk().execute(baseUrl, Collections.singletonMap("resource", this.resource), Collections.singletonMap("bulk", this.bulk));
        try {
            bulk.asJson();
            this.bulk.clear();
        } catch (UnirestException e) {
            e.printStackTrace();
        }
    }

    @Override
    public BaseRequest addElement(E element) throws NoSuchElementException{
        Map<String, Object> fields = toFields(element);
        if (fields == null) throw new NoSuchElementException();
        Map<String, Object> stringObjectMap = fields
                .entrySet().stream()
                .map(entry -> {
                    String[] split = entry.getKey().split("\\.");
                    return new HashMap.SimpleEntry<>(split[split.length - 1], entry.getValue());
                }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<String, Object> urlMap = new HashMap<>();
        urlMap.putAll(stringObjectMap);
        urlMap.put("resource", resource);
        return insertElement(urlMap, stringObjectMap);
    }

    protected BaseRequest insertElement(Map<String, Object> urlMap, Map<String, Object> object){
        if (templateHolder.isBulk()){
            Map<String, Object> insert = new HashMap<>();
            insert.put("object", Collections.singletonMap("prop", object.entrySet()));
            insert.put("url", urlMap);
            bulk.add(insert);
            if (bulk.size() >= bulkSize)
                runBulk();
            return null;
        }

        if (templateHolder.isAdd())
            return templateHolder.getAdd().execute(baseUrl, urlMap, Collections.singletonMap("prop", object.entrySet()));
        return null;
    }

    @Override
    public BaseRequest delete(E element) {
        if(templateHolder.isDelete()) {
            Map<String, Object> urlMap = new HashMap<>();
            urlMap.put("id", element.id());
            urlMap.put("label", element.label());
            urlMap.put("resource", resource);
            element.properties().forEachRemaining(prop -> urlMap.put(prop.key(), prop.value()));
            return templateHolder.getDelete().execute(baseUrl, urlMap, element);
        }
        return null;
    }
}
