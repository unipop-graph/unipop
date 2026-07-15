package org.unipop.jdbc.schemas.property;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.json.JSONObject;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.schema.property.AbstractPropertyContainer;
import org.unipop.schema.property.PropertySchema;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A keyed property schema backing one PostgreSQL JSONB column. The property key is the column name
 * and the schema owns the {@code <column>.<path>} namespace: writes route those properties into the
 * column's JSON at the dotted path, reads expand the column's top-level keys back prefixed, and
 * queries on {@code <column>.<path>} pass through to the jdbc translator (which renders col->>'k').
 * Declare one per column: {@code "data": {"type": "jsonb"}}.
 */
public class JsonbPropertySchema implements PropertySchema, JsonbColumnSchema {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final String column;

    public JsonbPropertySchema(String column) {
        this.column = column;
    }

    @Override
    public String getKey() {
        return column;
    }

    @Override
    public String getJsonbColumn() {
        return column;
    }

    /** True if {@code key} is addressed under this column, i.e. {@code <column>.<path>}. */
    private boolean owns(String key) {
        int dot = key.indexOf('.');
        return dot > 0 && key.substring(0, dot).equals(column);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> toProperties(Map<String, Object> source) {
        Object raw = source.get(column);
        if (raw == null) return Collections.emptyMap();
        try {
            Object parsed = MAPPER.readValue(raw.toString(), Object.class);
            if (!(parsed instanceof Map)) return Collections.emptyMap();
            Map<String, Object> props = new HashMap<>();
            Map<String, Object> whole = (Map<String, Object>) parsed;
            props.put(column, whole);
            whole.forEach((k, v) -> {
                if (v != null) props.put(column + "." + k, v);
            });
            return props;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse JSON column '" + column + "'", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> toFields(Map<String, Object> properties) {
        Map<String, Object> root = new HashMap<>();
        for (Map.Entry<String, Object> e : properties.entrySet()) {
            if (!owns(e.getKey())) continue;
            String[] segs = e.getKey().substring(column.length() + 1).split("\\.", -1);
            Map<String, Object> cur = root;
            for (int i = 0; i < segs.length - 1; i++) {
                cur = (Map<String, Object>) cur.computeIfAbsent(segs[i], s -> new HashMap<String, Object>());
            }
            cur.put(segs[segs.length - 1], e.getValue());
        }
        if (root.isEmpty()) return Collections.emptyMap();
        try {
            return Collections.singletonMap(column, MAPPER.writeValueAsString(root));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize JSON column '" + column + "'", e);
        }
    }

    @Override
    public Set<String> toFields(Set<String> propertyKeys) {
        return Collections.singleton(column);
    }

    @Override
    public Set<Object> getValues(PredicatesHolder predicatesHolder) {
        // null matches DynamicPropertySchema/Coalesce; this schema is never nested as a Multi child,
        // so the null-unsafe MultiPropertySchema.getValues path is unreachable.
        return null;
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        Set<HasContainer> owned = predicatesHolder.getPredicates().stream()
                .filter(has -> owns(has.getKey()))
                .collect(Collectors.toSet());
        return PredicatesHolderFactory.createFromPredicates(predicatesHolder.getClause(), owned);
    }

    @Override
    public Set<String> excludeDynamicFields() {
        return Collections.singleton(column);
    }

    @Override
    public Set<String> excludeDynamicPropertyPrefixes() {
        return Collections.singleton(column);
    }

    public static class Builder implements PropertySchemaBuilder {
        @Override
        public PropertySchema build(String key, Object conf, AbstractPropertyContainer container) {
            if (!(conf instanceof JSONObject)) return null;
            JSONObject config = (JSONObject) conf;
            if (!"jsonb".equalsIgnoreCase(config.optString("type", ""))) return null;
            return new JsonbPropertySchema(key);
        }
    }
}
