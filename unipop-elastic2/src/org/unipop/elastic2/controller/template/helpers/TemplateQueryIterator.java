package org.unipop.elastic2.controller.template.helpers;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.Template;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.unipop.elastic2.helpers.TimingAccessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by sbarzilay on 02/02/16.
 *
 */
public class TemplateQueryIterator<E extends Element> implements Iterator<E> {
    private long allowedRemaining;
    private final Parser<E> parser;
    Iterator<JSONObject> objectIterator;

    @SuppressWarnings("unchecked")
    public TemplateQueryIterator(long maxSize,
                                 Client client,
                                 Parser<E> parser,
                                 TimingAccessor timing,
                                 String templateName,
                                 Map<String, Object> templateParams,
                                 ScriptService.ScriptType type,
                                 Map<String, String> paths,
                                 String... indices) {
        this.allowedRemaining = maxSize;
        this.parser = parser;
        timing.start("template");

        SearchRequestBuilder searchRequestBuilder;


        searchRequestBuilder = client.prepareSearch(indices).setTemplate(new Template(templateName, ScriptService.ScriptType.FILE,"mustache" ,null,templateParams));


        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        XContentBuilder builder;
        try {
            builder = XContentFactory.jsonBuilder();
            builder.startObject();
            searchResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            JSONObject jsonResponse = (JSONObject) JSONValue.parse(builder.string());
            List<JSONObject> objects = new ArrayList<>();
            paths.keySet().forEach(path ->{
                String[] seg = path.split("\\.");
                JSONObject ele = jsonResponse;
                boolean shouldAdd = true;
                for (String element : seg){
                    Object temp = ele.get(element);
                    if (temp instanceof JSONObject)
                        ele = ((JSONObject) temp);
                    else if (temp instanceof JSONArray) {
                        ((JSONArray) temp).forEach(jsonElement -> {
                            JSONObject object = ((JSONObject) jsonElement);
                            object.put("label", paths.get(path));
                            objects.add(object);
                        });
                        shouldAdd = false;
                    }
                }
                if (shouldAdd)
                    objects.add(ele);
                else shouldAdd = true;
            });
            objectIterator = objects.iterator();
        } catch (IOException e) {
            throw new RuntimeException("something went wrong");
        }

        timing.stop("template");
    }

    @Override
    public boolean hasNext() {
        return allowedRemaining > 0 && objectIterator.hasNext();
    }

    @SuppressWarnings("unchecked")
    @Override
    public E next() {
        allowedRemaining--;
        Map<String, Object> object = objectIterator.next();
        if (object.containsKey("_source")){
            Map<String, Object> source = ((Map<String, Object>) object.get("_source"));
            return parser.parse(object.get("_id").toString(), object.get("_type").toString(), source);
        }
        else{
            return parser.parse(object.hashCode(), object.get("label").toString(), object);
        }
    }

    public interface Parser<E> {
        E parse(Object id, String label, Map<String, Object> keyValues);
    }
}
