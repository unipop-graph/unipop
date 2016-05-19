package org.unipop.elastic;

import com.google.common.collect.Sets;
import org.elasticsearch.client.Client;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.common.schema.referred.ReferredVertexSchema;
import org.unipop.common.property.PropertySchemaFactory;
import org.unipop.common.property.PropertySchema;
import org.unipop.elastic.common.ElasticClientFactory;
import org.unipop.elastic.common.ElasticHelper;
import org.unipop.elastic.document.DocumentController;
import org.unipop.elastic.document.schema.DocEdgeSchema;
import org.unipop.elastic.document.schema.DocVertexSchema;
import org.unipop.query.controller.SourceProvider;
import org.unipop.query.controller.UniQueryController;
import org.unipop.structure.UniGraph;

import java.util.*;

public class ElasticSourceProvider implements SourceProvider {

    private Client client;

    @Override
    public Set<UniQueryController> init(UniGraph graph, JSONObject configuration) throws Exception {
        this.client = ElasticClientFactory.create(configuration);

        Set<DocVertexSchema> docVertexSchemas = new HashSet<>();
        List<JSONObject> vertices = getConfigs(configuration, "vertices");
        for(JSONObject vertex : vertices) {
            String index = vertex.getString("index");
            String type = vertex.optString("_type");
            ArrayList<PropertySchema> propertySchemas = PropertySchemaFactory.createPropertySchemas(vertex);
            DocVertexSchema vertexSchema = new DocVertexSchema(index, type, propertySchemas, graph);
            docVertexSchemas.add(vertexSchema);
            ElasticHelper.createIndex(index, client);
        }

        Set<DocEdgeSchema> docEdgeSchemas = new HashSet<>();
        List<JSONObject> edges = getConfigs(configuration, "edges");
        for(JSONObject edge : edges) {
            JSONObject outVertex = edge.getJSONObject("outVertex");
            ArrayList<PropertySchema> outPropertySchemas = PropertySchemaFactory.createPropertySchemas(outVertex);
            ReferredVertexSchema outVertexSchema = new ReferredVertexSchema(outPropertySchemas, graph);

            JSONObject inVertex = edge.getJSONObject("inVertex");
            ArrayList<PropertySchema> inPropertySchemas = PropertySchemaFactory.createPropertySchemas(inVertex);
            ReferredVertexSchema inVertexSchema = new ReferredVertexSchema(inPropertySchemas, graph);

            String index = edge.getString("index");
            String type = edge.optString("_type");
            ArrayList<PropertySchema> propertySchemas = PropertySchemaFactory.createPropertySchemas(edge);
            DocEdgeSchema docEdgeSchema = new DocEdgeSchema(index, type, outVertexSchema, inVertexSchema, propertySchemas, graph);
            docEdgeSchemas.add(docEdgeSchema);
            ElasticHelper.createIndex(index, client);
        }

        DocumentController documentController = new DocumentController(this.client, docVertexSchemas, docEdgeSchemas, graph);
        return Sets.newHashSet(documentController);
    }

    private List<JSONObject> getConfigs(JSONObject configuration, String key) throws JSONException {
        List<JSONObject> configs = new ArrayList<>();
        JSONArray configsArray = configuration.optJSONArray(key);
        for(int i = 0; i < configsArray.length(); i++){
            JSONObject config = configsArray.getJSONObject(i);
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void close() {
        client.close();
    }
}
