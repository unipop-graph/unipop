package org.unipop.rest;

import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;
import org.json.JSONObject;
import org.unipop.query.controller.SourceProvider;
import org.unipop.query.controller.UniQueryController;
import org.unipop.rest.schema.RestEdge;
import org.unipop.rest.schema.RestVertex;
import org.unipop.rest.util.TemplateHolder;
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
    private String resultPath;
    private JSONObject opTranslator;
    private int maxResultSize;
    private TemplateHolder templateHolder;

    @Override
    public Set<UniQueryController> init(UniGraph graph, JSONObject configuration) throws Exception {
        this.graph = graph;
        String url = configuration.optString("baseUrl");
        templateHolder = new TemplateHolder(configuration);
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
        return new RestEdge(json, graph, url, templateHolder, resultPath, opTranslator, maxResultSize);
    }

    private RestSchema createVertexSchema(JSONObject json, String url) {
        return new RestVertex(json, url, graph, templateHolder, resultPath, opTranslator, maxResultSize);
    }

    @Override
    public void close() {

    }
}
