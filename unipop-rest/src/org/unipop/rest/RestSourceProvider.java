package org.unipop.rest;

import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;
import org.json.JSONObject;
import org.unipop.query.controller.SourceProvider;
import org.unipop.query.controller.UniQueryController;
import org.unipop.rest.schema.RestEdge;
import org.unipop.rest.schema.RestVertex;
import org.unipop.structure.UniGraph;

import java.io.*;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.unipop.util.ConversionUtils.getList;

/**
 * Created by sbarzilay on 24/11/16.
 */
public class RestSourceProvider implements SourceProvider{
    private UniGraph graph;
    private Template searchUrlTemplate;
    private Template searchTemplate;
    private Template addTemplate;
    private Template addUrlTemplate;
    private Template deleteUrlTemplate;
    private String resultPath;
    private JSONObject opTranslator;
    private int maxResultSize;

    private Reader getReader(String mustache){
        if (mustache.contains("{"))
            return new StringReader(mustache);
        try {
            return new FileReader(mustache);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("file: " + mustache + " not found");
        }
    }

    @Override
    public Set<UniQueryController> init(UniGraph graph, JSONObject configuration) throws Exception {
        this.graph = graph;
        String url = configuration.optString("baseUrl");
        JSONObject search = configuration.optJSONObject("search");
        JSONObject add = configuration.optJSONObject("add");
        JSONObject delete = configuration.optJSONObject("delete");
        String searchString = search.getString("template");
        this.searchTemplate = Mustache.compiler().escapeHTML(false).standardsMode(true).withLoader(s ->
            getReader(searchString)).compile(getReader(searchString));
        this.searchUrlTemplate = Mustache.compiler().compile(getReader(search.optString("url", "")));
        this.addUrlTemplate = Mustache.compiler().compile(getReader(add.optString("url")));
        this.addTemplate = Mustache.compiler().compile(getReader(add.optString("template")));
        this.deleteUrlTemplate = Mustache.compiler().compile(getReader(delete.optString("url")));
        this.resultPath = configuration.optString("resultPath");
        this.opTranslator = configuration.getJSONObject("opTranslator");
        this.maxResultSize = configuration.optInt("maxResultSize", 10000);

        Set<RestSchema> schemas = new HashSet<>();
        for(JSONObject json : getList(configuration, "vertices")) {
            schemas.add(createVertexSchema(json, url));
        }
        for(JSONObject json : getList(configuration, "edges")) {
            schemas.add(createEdgeSchema(json, url));
        }

        return Collections.singleton(new RestController(graph, schemas));
    }

    private RestSchema createEdgeSchema(JSONObject json, String url) {
        return new RestEdge(json, graph, url, searchTemplate, searchUrlTemplate, addTemplate, addUrlTemplate, deleteUrlTemplate, resultPath, opTranslator, maxResultSize);
    }

    private RestSchema createVertexSchema(JSONObject json, String url) {
        return new RestVertex(json, url, graph, searchTemplate, searchUrlTemplate, addTemplate, addUrlTemplate, deleteUrlTemplate, resultPath, opTranslator, maxResultSize);
    }

    @Override
    public void close() {

    }
}
