package org.unipop.rest;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import org.json.JSONObject;
import org.unipop.query.controller.SourceProvider;
import org.unipop.query.controller.UniQueryController;
import org.unipop.rest.schema.RestVertex;
import org.unipop.structure.UniGraph;

import java.io.StringReader;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.unipop.util.ConversionUtils.getList;

/**
 * Created by sbarzilay on 24/11/16.
 */
public class RestSourceProvider implements SourceProvider{
    private UniGraph graph;
    private Mustache searchUrlTemplate;
    private Mustache searchTemplate;
    private Mustache addTemplate;
    private Mustache addUrlTemplate;
    private Mustache deleteUrlTemplate;

    @Override
    public Set<UniQueryController> init(UniGraph graph, JSONObject configuration) throws Exception {
        this.graph = graph;
        String url = configuration.optString("baseUrl");


        MustacheFactory mf = new DefaultMustacheFactory();
        searchTemplate = mf.compile(new StringReader(configuration.optString("searchTemplate")), "search");
        searchUrlTemplate = mf.compile(new StringReader(configuration.optString("searchUrlTemplate", "")), "searchUrl");
        addTemplate = mf.compile(new StringReader(configuration.optString("addTemplate")), "add");
        addUrlTemplate = mf.compile(new StringReader(configuration.optString("addUrlTemplate")), "addUrl");
        deleteUrlTemplate = mf.compile(new StringReader(configuration.optString("deleteUrlTemplate")), "deleteUrl");

        Set<RestSchema> schemas = new HashSet<>();
        for(JSONObject json : getList(configuration, "vertices")) {
            schemas.add(createVertexSchema(json, url));
        }
        for(JSONObject json : getList(configuration, "edges")) {
            schemas.add(createEdgeSchema(json));
        }

        return Collections.singleton(new RestController(graph, schemas));
    }

    private RestSchema createEdgeSchema(JSONObject json) {
        return null;
    }

    private RestSchema createVertexSchema(JSONObject json, String url) {
        return new RestVertex(json, url, graph, searchTemplate, searchUrlTemplate, addTemplate, addUrlTemplate, deleteUrlTemplate);
    }

    @Override
    public void close() {

    }
}
