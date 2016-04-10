package org.unipop.common.schema.property;

import org.apache.commons.cli.MissingArgumentException;
import org.unipop.common.schema.property.FieldPropertySchema;
import org.unipop.common.schema.property.MultiFieldPropertySchema;
import org.unipop.common.schema.property.PropertySchema;
import org.unipop.common.schema.property.StaticPropertySchema;

import java.util.Map;

public class PropertyBuilder {
    public static PropertySchema createPropertySchema(String key, Object value) throws MissingArgumentException {
        if(value instanceof String) {
            if (key.startsWith("@"))
                return new FieldPropertySchema(key, value.toString().substring(1));
            else return new StaticPropertySchema(key, value.toString());
        }
        else if(value instanceof Map) {
            Map<String, Object> config = (Map<String, Object>) value;
            Object field = config.get("field");
            if(field != null && field instanceof String)
                return new FieldPropertySchema(key, config);
            else if(field != null && field instanceof String[])
                return new MultiFieldPropertySchema(key, (String[])field, "_");
        }
        throw new MissingArgumentException("");
    }
}
