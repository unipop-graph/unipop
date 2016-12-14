package org.unipop.rest;

import org.json.JSONArray;
import org.json.JSONObject;
import org.unipop.query.controller.SourceProvider;
import org.unipop.query.controller.UniQueryController;
import org.unipop.rest.schema.RestEdge;
import org.unipop.rest.schema.RestVertex;
import org.unipop.rest.util.MatcherHolder;
import org.unipop.rest.util.TemplateHolder;
import org.unipop.rest.util.matchers.KeyMatcher;
import org.unipop.rest.util.matchers.Matcher;
import org.unipop.rest.util.matchers.MultiOpMatcher;
import org.unipop.rest.util.matchers.OpMatcher;
import org.unipop.structure.UniGraph;
import org.unipop.structure.traversalfilter.TraversalFilter;

import java.util.*;

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
    private MatcherHolder complexTranslator;
    private boolean valuesToString;

    @Override
    public Set<UniQueryController> init(UniGraph graph, TraversalFilter filter, JSONObject configuration) throws Exception {
        this.graph = graph;
        String url = configuration.optString("baseUrl");
        templateHolder = new TemplateHolder(configuration);
        this.resultPath = configuration.optString("resultPath");
        this.opTranslator = configuration.getJSONObject("opTranslator");
        this.maxResultSize = configuration.optInt("maxResultSize", 10000);
        this.valuesToString = configuration.optBoolean("valuesToString", false);
        List<Matcher.MatcherBuilder> builders = new ArrayList<>();
        builders.add(new KeyMatcher.KeyMatcherBuilder());
        builders.add(new OpMatcher.OpMatcherBuilder());
        builders.add(new MultiOpMatcher.MultiOpMatcherBuilder());
        if (configuration.has("builders")){
            JSONArray builderNames = configuration.getJSONArray("builders");
            for (int i = 0; i < builderNames.length(); i++) {
                String builderName = builderNames.getString(i);
                builders.add(Class.forName(builderName).asSubclass(Matcher.MatcherBuilder.class).newInstance());
            }
        }
        this.complexTranslator = new MatcherHolder(configuration, builders);

        Set<RestSchema> schemas = new HashSet<>();
        for(JSONObject json : getList(configuration, "vertices")) {
            schemas.add(createVertexSchema(json, url));
        }
        for(JSONObject json : getList(configuration, "edges")) {
            schemas.add(createEdgeSchema(json, url));
        }

        return Collections.singleton(new RestController(graph, schemas, filter));
    }

    private RestSchema createEdgeSchema(JSONObject json, String url) {
        return new RestEdge(json, graph, url, templateHolder, resultPath, opTranslator, maxResultSize, complexTranslator, valuesToString);
    }

    private RestSchema createVertexSchema(JSONObject json, String url) {
        return new RestVertex(json, url, graph, templateHolder, resultPath, opTranslator, maxResultSize, complexTranslator, valuesToString);
    }

    @Override
    public void close() {

    }
}
