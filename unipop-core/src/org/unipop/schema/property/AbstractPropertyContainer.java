package org.unipop.schema.property;

import org.apache.tinkerpop.gremlin.structure.T;
import org.json.JSONArray;
import org.json.JSONObject;
import org.unipop.structure.UniGraph;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractPropertyContainer {
    protected final UniGraph graph;
    protected final JSONObject json;
    protected ArrayList<PropertySchema> propertySchemas = new ArrayList<>();
    protected DynamicPropertySchema dynamicProperties;

    public AbstractPropertyContainer(JSONObject json, UniGraph graph) {
        this.json = json;
        this.graph = graph;
        createPropertySchemas();
    }

    protected List<PropertySchema> getPropertySchemas() {
        return propertySchemas;
    }

    protected void createPropertySchemas() {
        addPropertySchema(T.id.getAccessor(), json.get(T.id.toString()), false);
        addPropertySchema(T.label.getAccessor(), json.get(T.label.toString()), false);

        JSONObject properties = json.optJSONObject("properties");
        if(properties != null) {
            properties.keys().forEachRemaining(key -> addPropertySchema(key, properties.get(key), true));
        }

        Object dynamicPropertiesConfig = json.opt("dynamicProperties");
        if(dynamicPropertiesConfig instanceof Boolean && (boolean)dynamicPropertiesConfig)
            this.dynamicProperties = new DynamicPropertySchema(propertySchemas);
        else if(dynamicPropertiesConfig instanceof JSONObject)
            this.dynamicProperties = new DynamicPropertySchema(propertySchemas, (JSONObject) dynamicPropertiesConfig);
        else this.dynamicProperties = new NonDynamicPropertySchema(propertySchemas);

        propertySchemas.add(this.dynamicProperties);
    }

    protected void addPropertySchema(String key, Object value, boolean nullable) {
        PropertySchema propertySchema = createPropertySchema(key, value, nullable);
        propertySchemas.add(propertySchema);
    }


    @Override
    public String toString() {
        return "AbstractPropertyContainer{" +
                "dynamicProperties=" + dynamicProperties +
                ", graph=" + graph +
                ", json=" + json +
                ", propertySchemas=" + propertySchemas +
                '}';
    }

    protected PropertySchema createPropertySchema(String key, Object value, boolean nullable) {
        if(value instanceof String) {
            if (value.toString().startsWith("@"))
                return new FieldPropertySchema(key, value.toString().substring(1), nullable);
            else return new StaticPropertySchema(key, value.toString());
        }
        else if(value instanceof JSONObject) {
            JSONObject config = (JSONObject) value;
            Object field = config.get("field");
            if(field != null && field instanceof String) {
                return new FieldPropertySchema(key, config, nullable);
            }
            else if(field instanceof JSONArray) {
                String delimiter = config.optString("delimiter", "_");
                return getMultiFieldProperty(key, (JSONArray) field, delimiter, nullable);
            }
            else throw new IllegalArgumentException("Unrecognized field: " + field + ", property: " + key + " - " + value);
        }
        else if(value instanceof JSONArray) {
            return getMultiFieldProperty(key, (JSONArray) value, "_", nullable);
        }
        else throw new IllegalArgumentException("Unrecognized property: " + key + " - " + value);
    }

    protected PropertySchema getMultiFieldProperty(String key, JSONArray fieldsArray, String delimiter, boolean nullable) {
        List<String> fields = new ArrayList<>();
        for(int i = 0; i < fieldsArray.length(); i++){
            String field = fieldsArray.getString(i);
            fields.add(field);
        }
        return new MultiFieldPropertySchema(key, fields, delimiter, nullable);
    }
}
