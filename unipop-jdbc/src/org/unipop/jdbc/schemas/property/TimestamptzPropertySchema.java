package org.unipop.jdbc.schemas.property;

import org.json.JSONObject;
import org.unipop.schema.property.AbstractPropertyContainer;
import org.unipop.schema.property.FieldPropertySchema;
import org.unipop.schema.property.PropertySchema;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

/**
 * A keyed property schema backing a PostgreSQL native {@code timestamptz} column. Extends
 * {@link FieldPropertySchema}; the column is reported via {@link TimestamptzColumnSchema} so the jdbc
 * translator compares it as a native temporal (never text-cast, which would break ordering). Values
 * are coerced to {@link OffsetDateTime} on write and read, so the column round-trips and
 * {@code values(col)} is always an {@code OffsetDateTime}. Declare: {@code "at": {"type":"timestamptz"}}.
 */
public class TimestamptzPropertySchema extends FieldPropertySchema implements TimestamptzColumnSchema {

    public TimestamptzPropertySchema(String key, String field, boolean nullable) {
        super(key, field, nullable);
    }

    @Override
    public String getTimestamptzColumn() {
        return field;
    }

    /**
     * Coerce a supported value to an {@link OffsetDateTime} (UTC for zone-less inputs).
     * Accepts OffsetDateTime (passthrough), Instant, java.util.Date (incl. java.sql.Timestamp), and
     * ISO-8601 String. Returns {@code null} for {@code null}. Throws {@link IllegalArgumentException}
     * for any other type.
     */
    public static OffsetDateTime toOffsetDateTime(Object value) {
        if (value == null) return null;
        if (value instanceof OffsetDateTime) return (OffsetDateTime) value;
        if (value instanceof Instant) return ((Instant) value).atOffset(ZoneOffset.UTC);
        if (value instanceof Date) return ((Date) value).toInstant().atOffset(ZoneOffset.UTC);
        if (value instanceof String) {
            String s = (String) value;
            try {
                return OffsetDateTime.parse(s);
            } catch (DateTimeParseException e) {
                try {
                    return LocalDateTime.parse(s).atOffset(ZoneOffset.UTC);
                } catch (DateTimeParseException e2) {
                    throw new IllegalArgumentException("Cannot coerce String to timestamptz: " + s, e2);
                }
            }
        }
        throw new IllegalArgumentException("Cannot coerce " + value.getClass().getName() + " to timestamptz: " + value);
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> properties) {
        Map<String, Object> fields = super.toFields(properties);
        if (fields == null || fields.isEmpty()) return fields;
        Object value = fields.get(field);
        if (value != null) {
            return Collections.singletonMap(field, toOffsetDateTime(value));
        }
        return fields;
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        Map<String, Object> props = super.toProperties(source);
        if (props == null || props.isEmpty()) return props;
        Object value = props.get(key);
        if (value != null) {
            return Collections.singletonMap(key, toOffsetDateTime(value));
        }
        return props;
    }

    public static class Builder implements PropertySchemaBuilder {
        @Override
        public PropertySchema build(String key, Object conf, AbstractPropertyContainer container) {
            if (!(conf instanceof JSONObject)) return null;
            JSONObject config = (JSONObject) conf;
            if (!"timestamptz".equalsIgnoreCase(config.optString("type", ""))) return null;
            String field = config.optString("field", key);
            if (field.startsWith("@")) field = field.substring(1);
            boolean nullable = config.optBoolean("nullable", true);
            return new TimestamptzPropertySchema(key, field, nullable);
        }
    }
}
