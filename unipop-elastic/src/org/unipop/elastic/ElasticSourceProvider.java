package org.unipop.elastic;

import com.google.common.collect.Sets;
import org.apache.commons.configuration.ConfigurationMap;
import org.apache.tinkerpop.gremlin.structure.T;
import org.elasticsearch.client.Client;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.common.schema.property.PropertySchema;
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
            DocVertexSchema docVertexSchema = new DocVertexSchema(index, type, properties, dynamicProperties, graph);
            docVertexSchemas.add(docVertexSchema);
        }

        Set<DocEdgeSchema> docEdgeSchemas = new HashSet<>();
        List<JSONObject> edges = getConfigs(configuration, "vertices");
        for(JSONObject edge : vertices) {
            create
            String index = edge.getString("index");
            String type = edge.optString("_type");
            Map<String, PropertySchema> properties = getProperties(edge);
            boolean dynamicProperties = edge.optBoolean("dynamicProperties", false);
            DocEdgeSchema docEdgeSchema = new DocEdgeSchema(index, type, properties, dynamicProperties, graph);
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

    private Map<String, PropertySchema> getProperties(JSONObject elementConfig) {
        addPropertySchema(T.id.getAccessor(), configuration.getProperty(T.id.getAccessor()));
        addPropertySchema(T.label.getAccessor(), configuration.getProperty(T.label.getAccessor()));
        ConfigurationMap properties = new ConfigurationMap(configuration.subset("properties"));
        for(Map.Entry<Object, Object> property : properties.entrySet()) {
            addPropertySchema(property.getKey().toString(), property.getValue());
        }
        return null;
    }

    @Override
    public void close() {
        client.close();
    }
}
