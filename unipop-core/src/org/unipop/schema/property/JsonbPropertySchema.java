package org.unipop.schema.property;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.json.JSONObject;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A dynamic-property catch-all that stores all unmapped properties inside a single JSON column
 * (a PostgreSQL JSONB column for the jdbc backend). Unmapped properties are serialized to one JSON
 * object on write and expanded back to properties on read (nested objects/arrays become Map/List
 * values). JSON keys are queryable, including nested paths — the jdbc translator renders the
 * Postgres path extraction ({@code col->>'k'} / {@code col->'a'->>'b'}).
 */
public class JsonbPropertySchema extends DynamicPropertySchema {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final String jsonbColumn;

    public JsonbPropertySchema(ArrayList<PropertySchema> otherSchemas, JSONObject config) {
        super(otherSchemas, config);
        this.jsonbColumn = config.getString("jsonb");
        this.excludeFields.add(jsonbColumn);
    }

    public String getJsonbColumn() {
        return jsonbColumn;
    }

    @Override
    public Set<String> getJsonbColumns() {
        return Collections.singleton(jsonbColumn);
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        Object raw = source.get(jsonbColumn);
        if (raw == null) return Collections.emptyMap();
        try {
            Map<String, Object> parsed = MAPPER.readValue(raw.toString(), Map.class);
            return parsed.entrySet().stream()
                    .filter(e -> !excludeFields.contains(e.getKey()) && e.getValue() != null)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse JSON column '" + jsonbColumn + "'", e);
        }
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> properties) {
        Map<String, Object> dynamic = properties.entrySet().stream()
                .filter(e -> !excludeProperties.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (dynamic.isEmpty()) return Collections.emptyMap();
        try {
            return Collections.singletonMap(jsonbColumn, MAPPER.writeValueAsString(dynamic));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize JSON column '" + jsonbColumn + "'", e);
        }
    }

    @Override
    public Set<String> toFields(Set<String> propertyKeys) {
        return Collections.singleton(jsonbColumn);
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        Set<HasContainer> rewritten = predicatesHolder.getPredicates().stream()
                .filter(has -> !excludeProperties.contains(has.getKey()))
                .map(has -> new HasContainer(jsonbColumn + "." + has.getKey(), has.getPredicate()))
                .collect(Collectors.toSet());
        return PredicatesHolderFactory.createFromPredicates(predicatesHolder.getClause(), rewritten);
    }
}
