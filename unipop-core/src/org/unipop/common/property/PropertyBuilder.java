package org.unipop.common.property;

import org.json.JSONObject;

public class PropertyBuilder {
    public static PropertySchema createPropertySchema(String key, Object value) {
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
