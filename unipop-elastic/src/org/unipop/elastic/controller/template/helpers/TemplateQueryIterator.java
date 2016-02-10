package org.unipop.elastic.controller.template.helpers;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ContentHandler;
import org.json.simple.parser.JSONParser;
import org.unipop.elastic.helpers.TimingAccessor;

import java.io.IOException;
import java.util.*;

/**
 * Created by sbarzilay on 02/02/16.
 */
public class TemplateQueryIterator<E extends Element> implements Iterator<E> {
    private SearchResponse scrollResponse;
    private long allowedRemaining;
    private final Parser<E> parser;
    private TimingAccessor timing;
    private final int scrollSize;
    private Client client;
    private Iterator<SearchHit> hits;
    private Set<String> paths;
    Iterator<JSONObject> objectIterator;

    public TemplateQueryIterator(int scrollSize, long maxSize, Client client,
                                 Parser<E> parser, TimingAccessor timing, String templateName, Map<String, Object> templateParams, ScriptService.ScriptType type, Set<String> paths, String... indices) {
        this.scrollSize = scrollSize;
        this.client = client;
        this.allowedRemaining = maxSize;
        this.parser = parser;
        this.timing = timing;
        this.paths = paths;
        this.timing.start("template");

        SearchRequestBuilder searchRequestBuilder;

        searchRequestBuilder = client.prepareSearch(indices)
                .setTemplateName(templateName).setTemplateType(type)
                .setTemplateParams(templateParams);

        if (scrollSize > 0)
            searchRequestBuilder.setScroll(new TimeValue(60000))
                    .setSize(maxSize < scrollSize ? (int) maxSize : scrollSize);
        else searchRequestBuilder.setSize(maxSize < 100000 ? (int) maxSize : 100000);

        this.scrollResponse = searchRequestBuilder.execute().actionGet();

        hits = scrollResponse.getHits().iterator();
        XContentBuilder builder = null;
        try {
            builder = XContentFactory.jsonBuilder();
            builder.startObject();
            scrollResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            JsonParser json = new JsonParser();
            JSONObject jsonResponse = (JSONObject) JSONValue.parse(builder.string());
            List<JSONObject> objects = new ArrayList<>();
            paths.forEach(path ->{
                String[] seg = path.split("\\.");
                JSONObject ele = jsonResponse;
                boolean shouldAdd = true;
                for (String element : seg){
                    Object temp = ele.get(element);
                    if (temp instanceof JSONObject)
                        ele = ((JSONObject) temp);
                    else if (temp instanceof JSONArray) {
                        ((JSONArray) temp).forEach(jsonElement -> objects.add(((JSONObject) jsonElement)));
                        shouldAdd = false;
                    }
                }
                if (shouldAdd)
                    objects.add(ele);
                else shouldAdd = true;
            });
            objectIterator = objects.iterator();
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.timing.stop("template");
    }

    @Override
    public boolean hasNext() {
        if (allowedRemaining <= 0) return false;
        if (objectIterator.hasNext()) return true;
        return objectIterator.hasNext();
    }

    @Override
    public E next() {
        allowedRemaining--;
        JSONObject object = objectIterator.next();
        if (object.containsKey("_source")){
            Map<String, Object> source = ((Map<String, Object>) object.get("_source"));
            return parser.parse(object.get("_id").toString(), object.get("_type").toString(), source);
        }
        else{
            return parser.parse(object.hashCode(), "bucket", ((Map<String, Object>)object));
        }
    }

    public interface Parser<E> {
        E parse(Object id, String label, Map<String, Object> keyValues);
    }
}
