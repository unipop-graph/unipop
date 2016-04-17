package org.unipop.elastic;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.T;
import org.elasticsearch.client.Client;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.common.refer.ReferringVertexSchema;
import org.unipop.common.property.PropertyBuilder;
import org.unipop.common.property.PropertySchema;
import org.unipop.elastic.common.ElasticClientFactory;
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
            Map<String, PropertySchema> properties = getProperties(vertex);
            boolean dynamicProperties = vertex.optBoolean("dynamicProperties", false);
            DocVertexSchema vertexSchema = new DocVertexSchema(index, type, properties, dynamicProperties, graph);
            docVertexSchemas.add(vertexSchema);
        }

        Set<DocEdgeSchema> docEdgeSchemas = new HashSet<>();
        List<JSONObject> edges = getConfigs(configuration, "edges");
        for(JSONObject edge : edges) {
            JSONObject outVertex = edge.getJSONObject("outVertex");
            Map<String, PropertySchema> outVertexProperties = getProperties(outVertex);
            ReferringVertexSchema outVertexSchema = new ReferringVertexSchema(outVertexProperties, graph);

            JSONObject inVertex = edge.getJSONObject("inVertex");
            Map<String, PropertySchema> inVertexProperties = getProperties(inVertex);
            ReferringVertexSchema inVertexSchema = new ReferringVertexSchema(inVertexProperties, graph);

            String index = edge.getString("index");
            String type = edge.optString("_type");
            Map<String, PropertySchema> properties = getProperties(edge);
            boolean dynamicProperties = edge.optBoolean("dynamicProperties", false);
            DocEdgeSchema docEdgeSchema = new DocEdgeSchema(index, type, outVertexSchema, inVertexSchema, properties, dynamicProperties, graph);
            docEdgeSchemas.add(docEdgeSchema);
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

    private Map<String, PropertySchema> getProperties(JSONObject elementConfig) throws JSONException {
        Map<String, JSONObject> propertiesConfig = new HashMap<>();
        propertiesConfig.put(T.id.getAccessor(), elementConfig.getJSONObject(T.id.getAccessor()));
        propertiesConfig.put(T.label.getAccessor(), elementConfig.getJSONObject(T.label.getAccessor()));

        JSONObject properties = elementConfig.getJSONObject("properties");
        properties.keys().forEachRemaining(key -> {
            try {
                propertiesConfig.put(key.toString(), properties.getJSONObject(key.toString()));
            } catch (JSONException e) {
                e.printStackTrace();
            }
        });

        Map<String, PropertySchema> result = new HashMap<>();
        propertiesConfig.forEach((key, value) -> {
            PropertySchema propertySchema = PropertyBuilder.createPropertySchema(key, value);
            result.put(key, propertySchema);
        });
        return result;
    }

    @Override
    public void close() {
        client.close();
    }
}
