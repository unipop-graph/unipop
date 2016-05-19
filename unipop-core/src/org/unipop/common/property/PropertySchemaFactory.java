package org.unipop.common.property;

import org.apache.tinkerpop.gremlin.structure.T;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.common.property.impl.DynamicPropertiesSchema;
import org.unipop.common.property.impl.FieldPropertySchema;
import org.unipop.common.property.impl.MultiFieldPropertySchema;
import org.unipop.common.property.impl.StaticPropertySchema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class PropertySchemaFactory {

    public static ArrayList<PropertySchema> createPropertySchemas(JSONObject elementConfig) throws JSONException {
        ArrayList<PropertySchema> schemaProperties = new ArrayList<>();
        Set<String> excludeDynamic = new HashSet<>();
        excludeDynamic.add(T.id.toString());
        excludeDynamic.add(T.label.toString());

        schemaProperties.add(createPropertySchema(T.id.toString(), elementConfig));
        schemaProperties.add(createPropertySchema(T.label.toString(), elementConfig));

        JSONObject properties = elementConfig.optJSONObject("properties");
        if(properties != null) {
            properties.keys().forEachRemaining(key -> {
                PropertySchema propertySchema = createPropertySchema(key, properties);
                schemaProperties.add(propertySchema);
                excludeDynamic.add(key);

            });
        }

        Object dynamicPropertiesConfig = elementConfig.opt("dynamicProperties");
        if(dynamicPropertiesConfig instanceof Boolean && (boolean)dynamicPropertiesConfig)
            schemaProperties.add(new DynamicPropertiesSchema(excludeDynamic));
        else if(dynamicPropertiesConfig instanceof JSONObject)
            schemaProperties.add(new DynamicPropertiesSchema(excludeDynamic, (JSONObject) dynamicPropertiesConfig));

        return schemaProperties;
    }

    public static PropertySchema createPropertySchema(String key, JSONObject elementConfig) {
        Object value = elementConfig.get(key);
        if(value instanceof String) {
            if (value.toString().startsWith("@"))
                return new FieldPropertySchema(key, value.toString().substring(1));
            else return new StaticPropertySchema(key, value.toString());
        }
        else if(value instanceof JSONObject) {
            JSONObject config = (JSONObject) value;
            Object field = config.get("field");
            if(field != null && field instanceof String)
                return new FieldPropertySchema(key, config);
            else if(field != null && field instanceof String[])
                return new MultiFieldPropertySchema(key, (String[])field, "_");
            else throw new IllegalArgumentException("Unrecognized field: " + field + ", property: " + key + " - " + value);
        }
        else throw new IllegalArgumentException("Unrecognized property: " + key + " - " + value);
    }
}
