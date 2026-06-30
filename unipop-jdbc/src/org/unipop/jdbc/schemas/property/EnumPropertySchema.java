package org.unipop.jdbc.schemas.property;

import org.json.JSONObject;
import org.unipop.schema.property.AbstractPropertyContainer;
import org.unipop.schema.property.FieldPropertySchema;
import org.unipop.schema.property.PropertySchema;

/**
 * A keyed property schema backing a PostgreSQL native enum column. Extends {@link FieldPropertySchema}
 * for the column&lt;-&gt;property mapping (read/write/predicate); the column is reported via
 * {@link EnumColumnSchema} so the jdbc translator compares it as text (col::text = ?). Declare:
 * {@code "status": {"type":"enum"}}. An optional {@code "enumType"} name is retained as metadata
 * (validated when present) but is unused at runtime.
 */
public class EnumPropertySchema extends FieldPropertySchema implements EnumColumnSchema {
    private final String enumType;

    public EnumPropertySchema(String key, String field, boolean nullable, String enumType) {
        super(key, field, nullable);
        this.enumType = enumType;
    }

    @Override
    public String getEnumColumn() {
        return field;
    }

    /** @return the optional declared PostgreSQL enum type name (metadata; may be null). */
    public String getEnumType() {
        return enumType;
    }

    public static class Builder implements PropertySchemaBuilder {
        @Override
        public PropertySchema build(String key, Object conf, AbstractPropertyContainer container) {
            if (!(conf instanceof JSONObject)) return null;
            JSONObject config = (JSONObject) conf;
            if (!"enum".equalsIgnoreCase(config.optString("type", ""))) return null;
            String field = config.optString("field", key);
            if (field.startsWith("@")) field = field.substring(1);
            String enumType = config.optString("enumType", null);
            if (enumType != null && !enumType.matches("^[A-Za-z_][A-Za-z0-9_]*$"))
                throw new IllegalArgumentException("Invalid enum type name in config: " + enumType);
            boolean nullable = config.optBoolean("nullable", true);
            return new EnumPropertySchema(key, field, nullable, enumType);
        }
    }
}
