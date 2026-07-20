package org.unipop.jdbc.utils;

import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unipop.jdbc.schemas.jdbc.JdbcSchema;
import org.unipop.jdbc.schemas.property.EnumPropertySchema;
import org.unipop.schema.element.ElementSchema;
import org.unipop.schema.property.AbstractPropertyContainer;
import org.unipop.schema.property.PropertySchema;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Loads PostgreSQL enum member labels from the catalog into {@link EnumPropertySchema}
 * so {@link org.unipop.schema.property.PropertySchema#knownValues()} is closed without
 * requiring config-side {@code values}/{@code include} lists.
 *
 * <p>Resolution order per schema:
 * <ol>
 *   <li>Skip if config already declared a non-empty domain.</li>
 *   <li>If {@code enumType} is set → {@code pg_type}/{@code pg_enum} by type name.</li>
 *   <li>Else if table + column known → resolve the column's enum type and read members.</li>
 * </ol>
 * Failures are logged and leave the domain open (no over-pruning).
 */
public final class EnumDomainLoader {
    private static final Logger logger = LoggerFactory.getLogger(EnumDomainLoader.class);

    private EnumDomainLoader() {}

    public static void hydrate(ContextManager contextManager, Collection<? extends ElementSchema> schemas) {
        if (contextManager == null || schemas == null || schemas.isEmpty()) return;
        Map<String, Set<Object>> byType = new HashMap<>();
        Set<ElementSchema> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (ElementSchema schema : schemas) {
            walk(contextManager, schema, tableOf(schema), byType, seen);
        }
    }

    private static void walk(ContextManager cm, ElementSchema schema, String table,
                             Map<String, Set<Object>> byType, Set<ElementSchema> seen) {
        if (schema == null || !seen.add(schema)) return;
        String localTable = tableOf(schema) != null ? tableOf(schema) : table;

        if (schema instanceof AbstractPropertyContainer) {
            for (PropertySchema p : ((AbstractPropertyContainer) schema).getPropertySchemas()) {
                if (p instanceof EnumPropertySchema) {
                    hydrateOne(cm, (EnumPropertySchema) p, localTable, byType);
                }
            }
        }
        for (Object child : schema.getChildSchemas()) {
            if (child instanceof ElementSchema) {
                walk(cm, (ElementSchema) child, localTable, byType, seen);
            }
        }
    }

    private static void hydrateOne(ContextManager cm, EnumPropertySchema enumSchema, String table,
                                   Map<String, Set<Object>> byType) {
        if (!enumSchema.knownValues().isEmpty()) return; // config wins

        Set<Object> domain = null;
        String typeName = enumSchema.getEnumType();
        if (typeName != null && !typeName.isEmpty()) {
            domain = byType.computeIfAbsent(typeName, t -> loadByTypeName(cm, t));
        } else if (table != null && enumSchema.getEnumColumn() != null) {
            String cacheKey = table + "." + enumSchema.getEnumColumn();
            domain = byType.computeIfAbsent(cacheKey, k -> loadByTableColumn(cm, table, enumSchema.getEnumColumn()));
        }

        if (domain != null && !domain.isEmpty()) {
            enumSchema.setKnownValuesIfEmpty(domain);
            logger.debug("Hydrated enum domain for {}.{} → {}", table, enumSchema.getKey(), domain);
        }
    }

    /**
     * {@code SELECT enumlabel FROM pg_type t JOIN pg_enum e ON … WHERE t.typname = ?}
     */
    public static Set<Object> loadByTypeName(ContextManager cm, String typeName) {
        try {
            List<Map<String, Object>> rows = cm.fetch(DSL.resultQuery(
                    "SELECT e.enumlabel AS enumlabel "
                            + "FROM pg_catalog.pg_type t "
                            + "JOIN pg_catalog.pg_enum e ON e.enumtypid = t.oid "
                            + "WHERE t.typname = {0} "
                            + "ORDER BY e.enumsortorder",
                    DSL.val(typeName)));
            return labels(rows);
        } catch (Exception e) {
            logger.warn("Failed to load enum members for type '{}': {}", typeName, e.toString());
            return Collections.emptySet();
        }
    }

    /**
     * Resolve enum labels for a table column (handles plain enum columns and domains over enums).
     */
    public static Set<Object> loadByTableColumn(ContextManager cm, String table, String column) {
        try {
            List<Map<String, Object>> rows = cm.fetch(DSL.resultQuery(
                    "SELECT e.enumlabel AS enumlabel "
                            + "FROM pg_catalog.pg_attribute a "
                            + "JOIN pg_catalog.pg_class c ON c.oid = a.attrelid "
                            + "JOIN pg_catalog.pg_type t ON t.oid = a.atttypid "
                            + "JOIN pg_catalog.pg_enum e ON e.enumtypid = CASE "
                            + "  WHEN t.typtype = 'e' THEN t.oid "
                            + "  ELSE t.typbasetype END "
                            + "WHERE c.relname = {0} AND a.attname = {1} "
                            + "  AND a.attnum > 0 AND NOT a.attisdropped "
                            + "ORDER BY e.enumsortorder",
                    DSL.val(table), DSL.val(column)));
            return labels(rows);
        } catch (Exception e) {
            logger.warn("Failed to load enum members for {}.{}: {}", table, column, e.toString());
            return Collections.emptySet();
        }
    }

    private static Set<Object> labels(List<Map<String, Object>> rows) {
        if (rows == null || rows.isEmpty()) return Collections.emptySet();
        Set<Object> out = new LinkedHashSet<>();
        for (Map<String, Object> row : rows) {
            Object v = row.get("enumlabel");
            if (v == null) {
                // jOOQ / driver may return uppercase keys depending on settings
                v = row.values().stream().findFirst().orElse(null);
            }
            if (v != null) out.add(v.toString());
        }
        return out.isEmpty() ? Collections.emptySet() : Collections.unmodifiableSet(out);
    }

    private static String tableOf(ElementSchema schema) {
        if (schema instanceof JdbcSchema) {
            String t = ((JdbcSchema) schema).getTable();
            return t == null || t.isEmpty() ? null : t;
        }
        return null;
    }
}
