package org.unipop.schema.property;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.json.JSONArray;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Dynamic-property schema backing N PostgreSQL JSONB columns, prefix-addressed: a property keyed
 * {@code <column>.<path>} (where column is one of the declared jsonb columns) is stored into that
 * column's JSON at the dotted path; reads expand each column's top-level keys back prefixed; queries
 * on {@code <column>.<path>} pass through to the jdbc translator. Unknown (non-jsonb, non-mapped)
 * keys abort the query, preserving NonDynamic semantics.
 */
public class MultiJsonbPropertySchema extends DynamicPropertySchema {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Set<String> jsonbColumns;

    public MultiJsonbPropertySchema(ArrayList<PropertySchema> otherSchemas, JSONArray columns) {
        super(otherSchemas);
        Set<String> cols = new LinkedHashSet<>();
        for (int i = 0; i < columns.length(); i++) cols.add(columns.getString(i));
        this.jsonbColumns = Collections.unmodifiableSet(cols);
        this.excludeFields.addAll(jsonbColumns);
    }

    @Override
    public Set<String> getJsonbColumns() {
        return jsonbColumns;
    }

    /** The declared jsonb column for a {@code col.path} key, or null. */
    private String columnOf(String key) {
        int dot = key.indexOf('.');
        if (dot <= 0) return null;
        String col = key.substring(0, dot);
        return jsonbColumns.contains(col) ? col : null;
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        Map<String, Object> props = new HashMap<>();
        for (String col : jsonbColumns) {
            Object raw = source.get(col);
            if (raw == null) continue;
            try {
                Map<String, Object> parsed = MAPPER.readValue(raw.toString(), Map.class);
                parsed.forEach((k, v) -> {
                    if (v != null) props.put(col + "." + k, v);
                });
            } catch (Exception e) {
                throw new IllegalStateException("Failed to parse JSON column '" + col + "'", e);
            }
        }
        return props;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> toFields(Map<String, Object> properties) {
        Map<String, Map<String, Object>> byColumn = new HashMap<>();
        for (Map.Entry<String, Object> e : properties.entrySet()) {
            String col = columnOf(e.getKey());
            if (col == null) continue;
            String[] segs = e.getKey().substring(col.length() + 1).split("\\.", -1);
            Map<String, Object> cur = byColumn.computeIfAbsent(col, c -> new HashMap<>());
            for (int i = 0; i < segs.length - 1; i++) {
                cur = (Map<String, Object>) cur.computeIfAbsent(segs[i], s -> new HashMap<String, Object>());
            }
            cur.put(segs[segs.length - 1], e.getValue());
        }
        if (byColumn.isEmpty()) return Collections.emptyMap();
        Map<String, Object> fields = new HashMap<>();
        byColumn.forEach((col, map) -> {
            try {
                fields.put(col, MAPPER.writeValueAsString(map));
            } catch (Exception e) {
                throw new IllegalStateException("Failed to serialize JSON column '" + col + "'", e);
            }
        });
        return fields;
    }

    @Override
    public Set<String> toFields(Set<String> propertyKeys) {
        return new HashSet<>(jsonbColumns);
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        Set<HasContainer> nonExcluded = predicatesHolder.getPredicates().stream()
                .filter(has -> !excludeProperties.contains(has.getKey()))
                .collect(Collectors.toSet());
        if (nonExcluded.stream().anyMatch(h -> columnOf(h.getKey()) == null)) {
            return PredicatesHolderFactory.abort();
        }
        return PredicatesHolderFactory.createFromPredicates(predicatesHolder.getClause(), nonExcluded);
    }
}
