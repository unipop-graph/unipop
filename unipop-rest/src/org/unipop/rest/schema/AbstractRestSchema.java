package org.unipop.rest.schema;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.request.BaseRequest;
import com.mashape.unirest.request.body.RequestBodyEntity;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.json.JSONObject;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.rest.RestSchema;
import org.unipop.schema.element.AbstractElementSchema;
import org.unipop.structure.UniGraph;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 24/11/16.
 */
public abstract class AbstractRestSchema <E extends Element> extends AbstractElementSchema<E> implements RestSchema <E>{

    private String baseUrl;
    private String resource;
    private Mustache searchUrlTemplate;
    private Mustache searchTemplate;
    private Mustache addTemplate;
    private Mustache addUrlTemplate;
    private Mustache deleteUrlTemplate;

    public AbstractRestSchema(JSONObject configuration, UniGraph graph, String url, Mustache searchTemplate, Mustache searchUrlTemplate, Mustache addTemplate, Mustache addUrlTemplate, Mustache deleteUrlTemplate) {
        super(configuration, graph);
        this.resource = configuration.optString("resource");
        this.searchTemplate = searchTemplate;
        this.searchUrlTemplate = searchUrlTemplate;
        this.addTemplate = addTemplate;
        this.addUrlTemplate = addUrlTemplate;
        this.deleteUrlTemplate = deleteUrlTemplate;
        this.baseUrl = url;
    }

    @Override
    public BaseRequest getSearch(SearchQuery<E> query) {
        List<HasContainer> predicates = query.getPredicates().getPredicates();

        List<HashMap<String, Object>> predicatesMaps = predicates.stream().map(has -> {
            HashMap<String, Object> map = new HashMap<>();
            map.put("key", has.getKey());
            map.put("value", has.getValue());
            map.put("op", has.getBiPredicate().toString());
            return map;
        }).collect(Collectors.toList());

        StringWriter body = new StringWriter();

        searchTemplate.execute(new StringWriter(), Collections.singletonMap("predicates", predicatesMaps));
        StringWriter url = new StringWriter();
        searchUrlTemplate.execute(url, predicatesMaps);
        RequestBodyEntity request = Unirest.post(baseUrl + resource + url.toString())
                .body(body.toString());

        return request;
    }

    @Override
    public List<E> parseResults(HttpResponse<JsonNode> result, PredicateQuery query) {
        throw new NotImplementedException("to be continue");
    }

    @Override
    public BaseRequest addElement(E element) {
        StringWriter url = new StringWriter();
        StringWriter elementBody = new StringWriter();
        Map<String, Object> stringObjectMap = toFields(element);
        addUrlTemplate.execute(url, stringObjectMap);

        addTemplate.execute(elementBody, Collections.singletonMap("prop", stringObjectMap.entrySet()));

        return Unirest.post(baseUrl + resource + url.toString()).body(elementBody.toString());
    }

    @Override
    public BaseRequest delete(E element) {
        StringWriter url = new StringWriter();
        deleteUrlTemplate.execute(url, element);
        return Unirest.delete(baseUrl + url.toString());
    }
}
