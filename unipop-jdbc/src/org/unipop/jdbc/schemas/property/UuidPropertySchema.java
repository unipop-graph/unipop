package org.unipop.jdbc.schemas.property;

import org.json.JSONObject;
import org.unipop.schema.property.AbstractPropertyContainer;
import org.unipop.schema.property.FieldPropertySchema;
import org.unipop.schema.property.PropertySchema;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * A keyed property schema backing a PostgreSQL native uuid column. Extends {@link FieldPropertySchema}
 * for the column&lt;-&gt;property mapping; the column is reported via {@link UuidColumnSchema} so the
 * jdbc translator compares it as text (col::text = ?). Values are coerced to {@link UUID} on write
 * (so the native uuid column binds a uuid, not a varchar) and on read (so {@code values(col)} yields
 * a {@code java.util.UUID} regardless of how the driver returns the column). Declare:
 * {@code "ref": {"type":"uuid"}}.
 */
public class UuidPropertySchema extends FieldPropertySchema implements UuidColumnSchema {

    public UuidPropertySchema(String key, String field, boolean nullable) {
        super(key, field, nullable);
    }

    @Override
    public String getUuidColumn() {
        return field;
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> properties) {
        Map<String, Object> fields = super.toFields(properties);
        if (fields == null || fields.isEmpty()) return fields;
        Object value = fields.get(field);
        if (value instanceof String) {
            return Collections.singletonMap(field, UUID.fromString((String) value));
        }
        return fields;
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        Map<String, Object> props = super.toProperties(source);
        if (props == null || props.isEmpty()) return props;
        Object value = props.get(key);
        if (value instanceof String) {
            return Collections.singletonMap(key, UUID.fromString((String) value));
        }
        return props;
    }

    public static class Builder implements PropertySchemaBuilder {
        @Override
        public PropertySchema build(String key, Object conf, AbstractPropertyContainer container) {
            if (!(conf instanceof JSONObject)) return null;
            JSONObject config = (JSONObject) conf;
            if (!"uuid".equalsIgnoreCase(config.optString("type", ""))) return null;
            String field = config.optString("field", key);
            if (field.startsWith("@")) field = field.substring(1);
            boolean nullable = config.optBoolean("nullable", true);
            return new UuidPropertySchema(key, field, nullable);
        }
    }
}
