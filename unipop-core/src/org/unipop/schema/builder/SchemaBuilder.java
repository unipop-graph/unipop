package org.unipop.schema.builder;

import org.apache.tinkerpop.gremlin.structure.T;
import org.json.JSONArray;
import org.json.JSONObject;
import org.unipop.schema.property.*;
import org.unipop.schema.ElementSchema;
import org.unipop.structure.UniGraph;

import java.util.ArrayList;
import java.util.List;

public abstract class SchemaBuilder<S extends ElementSchema> {
    protected final UniGraph graph;
    protected final JSONObject json;
    protected ArrayList<PropertySchema> propertySchemas = new ArrayList<>();

    public SchemaBuilder(JSONObject json, UniGraph graph) {
        this.json = json;
        this.graph = graph;
        createPropertySchemas();
    }

    public abstract S build();

    private void createPropertySchemas() {
        propertySchemas.add(createPropertySchema(T.id.getAccessor(), json.get(T.id.toString())));
        propertySchemas.add(createPropertySchema(T.label.getAccessor(), json.get(T.label.toString())));

        JSONObject properties = json.optJSONObject("properties");
        if(properties != null) {
            properties.keys().forEachRemaining(key -> {
                PropertySchema propertySchema = createPropertySchema(key, properties.get(key));
                propertySchemas.add(propertySchema);
            });
        }

        Object dynamicPropertiesConfig = json.opt("dynamicProperties");
        if(dynamicPropertiesConfig instanceof Boolean && (boolean)dynamicPropertiesConfig)
            propertySchemas.add(new DynamicPropertiesSchema(propertySchemas));
        else if(dynamicPropertiesConfig instanceof JSONObject)
            propertySchemas.add(new DynamicPropertiesSchema(propertySchemas, (JSONObject) dynamicPropertiesConfig));

    }

    private PropertySchema createPropertySchema(String key, Object value) {
        if(value instanceof String) {
            if (value.toString().startsWith("@"))
                return new FieldPropertySchema(key, value.toString().substring(1));
            else return new StaticPropertySchema(key, value.toString());
        }
        else if(value instanceof JSONObject) {
            JSONObject config = (JSONObject) value;
            Object field = config.get("field");
            if(field != null && field instanceof String) {
                return new FieldPropertySchema(key, config);
            }
            else if(field instanceof JSONArray) {
                String delimiter = config.optString("delimiter", "_");
                return getMultiFieldProperty(key, (JSONArray) value, delimiter);
            }
            else throw new IllegalArgumentException("Unrecognized field: " + field + ", property: " + key + " - " + value);
        }
        else if(value instanceof JSONArray) {
            return getMultiFieldProperty(key, (JSONArray) value, "_");
        }
        else throw new IllegalArgumentException("Unrecognized property: " + key + " - " + value);
    }

    private PropertySchema getMultiFieldProperty(String key, JSONArray fieldsArray, String delimiter) {
        List<String> fields = new ArrayList<>();
        for(int i = 0; i < fieldsArray.length(); i++){
            String field = fieldsArray.getString(i);
            fields.add(field);
        }
        return new MultiFieldPropertySchema(key, fields, delimiter);
    }
}
