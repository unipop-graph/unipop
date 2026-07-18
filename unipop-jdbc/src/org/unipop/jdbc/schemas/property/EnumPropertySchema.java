package org.unipop.jdbc.schemas.property;

import org.json.JSONObject;
import org.unipop.schema.property.AbstractPropertyContainer;
import org.unipop.schema.property.FieldPropertySchema;
import org.unipop.schema.property.PropertySchema;
import org.unipop.util.ConversionUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * A keyed property schema backing a PostgreSQL native enum column. Extends {@link FieldPropertySchema}
 * for the column&lt;-&gt;property mapping (read/write/predicate); the column is reported via
 * {@link EnumColumnSchema} so the jdbc translator compares it as text (col::text = ?). Declare:
 * {@code "status": {"type":"enum"}}. An optional {@code "enumType"} name is retained as metadata
 * (validated when present).
 * <p>
 * Closed domain for optimizers ({@link #knownValues()}):
 * <ul>
 *   <li>Config {@code values} / {@code include} if present (wins).</li>
 *   <li>Otherwise filled at provider init by {@code EnumDomainLoader} from {@code pg_enum}
 *       (by {@code enumType}, or by table+column when the type name is omitted).</li>
 * </ul>
 * When used as {@code ~label}, that domain becomes a closed label set in the schema catalog.
 */
public class EnumPropertySchema extends FieldPropertySchema implements EnumColumnSchema {
    private final String enumType;
    /** Config and/or DB-hydrated members; empty set means domain still open. */
    private volatile Set<Object> values;

    public EnumPropertySchema(String key, String field, boolean nullable, String enumType) {
        this(key, field, nullable, enumType, Collections.emptySet());
    }

    public EnumPropertySchema(String key, String field, boolean nullable, String enumType, Set<Object> values) {
        super(key, field, nullable);
        this.enumType = enumType;
        this.values = values == null || values.isEmpty()
                ? Collections.emptySet()
                : Collections.unmodifiableSet(new LinkedHashSet<>(values));
    }

    @Override
    public String getEnumColumn() {
        return field;
    }

    /** @return the optional declared PostgreSQL enum type name (metadata; may be null). */
    public String getEnumType() {
        return enumType;
    }

    /**
     * Known enum members (config and/or catalog introspection). Empty when still unknown.
     */
    public Set<Object> getEnumValues() {
        return values;
    }

    /**
     * Fill domain from Postgres introspection. No-op when config (or a prior hydrate) already
     * provided members — config always wins.
     */
    public void setKnownValuesIfEmpty(Collection<?> domain) {
        if (domain == null || domain.isEmpty()) return;
        if (values != null && !values.isEmpty()) return;
        Set<Object> copy = new LinkedHashSet<>();
        for (Object v : domain) {
            if (v != null) copy.add(v.toString());
        }
        if (!copy.isEmpty()) {
            this.values = Collections.unmodifiableSet(copy);
        }
    }

    @Override
    public Set<Object> knownValues() {
        if (values != null && !values.isEmpty()) return values;
        return super.knownValues();
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
            // Prefer explicit "values"; accept "include" as the same closed-domain list (matches FieldPropertySchema).
            Set<Object> domain = ConversionUtils.toSet(config, "values");
            if (domain.isEmpty()) domain = ConversionUtils.toSet(config, "include");
            return new EnumPropertySchema(key, field, nullable, enumType, domain);
        }
    }
}
