package org.unipop.rest;

import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;
import org.json.JSONObject;
import org.unipop.query.controller.SourceProvider;
import org.unipop.query.controller.UniQueryController;
import org.unipop.rest.schema.RestEdge;
import org.unipop.rest.schema.RestVertex;
import org.unipop.structure.UniGraph;

import java.io.File;
import java.io.FileReader;
import java.util.Collections;
import java.util.HashSet;
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

    @Override
    public Set<UniQueryController> init(UniGraph graph, JSONObject configuration) throws Exception {
        this.graph = graph;
        String url = configuration.optString("baseUrl");
        String searchString = configuration.optString("searchTemplate");
        if (searchString.contains("{"))
            this.searchTemplate = Mustache.compiler().escapeHTML(false).compile(searchString);
        else
            this.searchTemplate = Mustache.compiler().escapeHTML(false).standardsMode(true).withLoader(s ->
                    new FileReader(new File(searchString)))
                    .compile(new FileReader(searchString));
        this.searchUrlTemplate = Mustache.compiler().compile(configuration.optString("searchUrlTemplate", ""));
        this.addUrlTemplate = Mustache.compiler().compile(configuration.optString("addUrlTemplate"));
        this.addTemplate = Mustache.compiler().compile(configuration.optString("addTemplate"));
        this.deleteUrlTemplate = Mustache.compiler().compile(configuration.optString("deleteUrlTemplate"));
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
