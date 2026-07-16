package org.unipop.jdbc.utils;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.QueryPart;
import org.jooq.impl.DSL;
import org.unipop.common.util.PredicatesTranslator;
import org.unipop.jdbc.schemas.property.TimestamptzPropertySchema;
import org.unipop.process.predicate.Date;
import org.unipop.process.predicate.ExistsP;
import org.unipop.process.predicate.Text;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.util.MultiDateFormat;

import java.text.ParseException;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

/**
 * @author Gur Ronen
 * @since 6/14/2016
 */
public class JdbcPredicatesTranslator implements PredicatesTranslator<Condition> {

    private final Set<String> idFields;
    private final Set<String> enumColumns;
    private final Set<String> jsonbColumns;
    private final Set<String> uuidColumns;
    private final Set<String> timestamptzColumns;
    private final String alias;

    public JdbcPredicatesTranslator() {
        this(Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
    }

    public JdbcPredicatesTranslator(Set<String> idFields) {
        this(idFields, Collections.emptySet(), Collections.emptySet());
    }

    public JdbcPredicatesTranslator(Set<String> idFields, Set<String> enumColumns) {
        this(idFields, enumColumns, Collections.emptySet());
    }

    public JdbcPredicatesTranslator(Set<String> idFields, Set<String> enumColumns, Set<String> jsonbColumns) {
        this(idFields, enumColumns, jsonbColumns, Collections.emptySet());
    }

    public JdbcPredicatesTranslator(Set<String> idFields, Set<String> enumColumns, Set<String> jsonbColumns, Set<String> uuidColumns) {
        this(idFields, enumColumns, jsonbColumns, uuidColumns, Collections.emptySet());
    }

    public JdbcPredicatesTranslator(Set<String> idFields, Set<String> enumColumns, Set<String> jsonbColumns, Set<String> uuidColumns, Set<String> timestamptzColumns) {
        this(idFields, enumColumns, jsonbColumns, uuidColumns, timestamptzColumns, null);
    }

    public JdbcPredicatesTranslator(Set<String> idFields, Set<String> enumColumns, Set<String> jsonbColumns, Set<String> uuidColumns, Set<String> timestamptzColumns, String alias) {
        this.idFields = idFields == null ? Collections.emptySet() : idFields;
        this.enumColumns = enumColumns == null ? Collections.emptySet() : enumColumns;
        this.jsonbColumns = jsonbColumns == null ? Collections.emptySet() : jsonbColumns;
        this.uuidColumns = uuidColumns == null ? Collections.emptySet() : uuidColumns;
        this.timestamptzColumns = timestamptzColumns == null ? Collections.emptySet() : timestamptzColumns;
        this.alias = alias;
    }

    /** A dotted key whose first segment is a JSONB column, e.g. {@code data.address.city}. */
    private boolean isJsonbKey(String key) {
        int dot = key.indexOf('.');
        return dot > 0 && jsonbColumns.contains(key.substring(0, dot));
    }

    /** Column name, qualified with {@link #alias} (e.g. {@code v.col}) when one is set. */
    private org.jooq.Name col(String key) {
        return alias == null ? DSL.name(key) : DSL.name(alias, key);
    }

    /**
     * The jOOQ field for a column. PostgreSQL enum columns are cast to text; JSONB catch-all keys
     * render as path extraction (col->>'k' / col->'a'->>'b'), comparing the (String) value as text.
     * Key segments are bound as parameters (no SQL injection).
     */
    private Field<Object> columnField(String key) {
        if (isJsonbKey(key)) {
            String[] parts = key.split("\\.", -1);
            QueryPart[] qps = new QueryPart[parts.length];
            qps[0] = DSL.field(col(parts[0]));
            StringBuilder sql = new StringBuilder("{0}");
            for (int i = 1; i < parts.length; i++) {
                qps[i] = DSL.val(parts[i]);
                sql.append(i == parts.length - 1 ? " ->> {" + i + "}" : " -> {" + i + "}");
            }
            return DSL.field(sql.toString(), Object.class, qps);
        }
        return (enumColumns.contains(key) || uuidColumns.contains(key))
                ? DSL.field("{0}::text", Object.class, DSL.field(col(key)))
                : DSL.field(col(key));
    }

    /**
     * Coerce id-column predicate values to String. The id column is VARCHAR but Gremlin ids
     * arrive as numbers (e.g. g.V(1)); PostgreSQL refuses to compare {@code varchar = bigint}
     * (H2 coerced implicitly). Only id-column values are stringified — numeric value columns
     * such as age/weight must keep their type.
     */
    private static Object stringifyValue(Object value) {
        if (value instanceof Collection) {
            return ((Collection<?>) value).stream()
                    .map(v -> v == null ? null : v.toString())
                    .collect(Collectors.toList());
        }
        return value == null ? null : value.toString();
    }

    private static Object coerceTemporal(Object value) {
        if (value instanceof Collection) {
            return ((Collection<?>) value).stream()
                    .map(v -> v == null ? null : TimestamptzPropertySchema.toOffsetDateTime(v))
                    .collect(Collectors.toList());
        }
        return value == null ? null : TimestamptzPropertySchema.toOffsetDateTime(value);
    }

    @Override
    public Condition translate(PredicatesHolder predicatesHolder) {
        Set<Condition> predicateFilters = predicatesHolder.getPredicates().stream()
                .map(this::extractCondition).collect(Collectors.toSet());
        Set<Condition> childFilters = predicatesHolder.getChildren().stream()
                .map(this::translate).collect(Collectors.toSet());
        predicateFilters.addAll(childFilters);

        if (predicateFilters.size() == 0) return DSL.trueCondition();
        if (predicateFilters.size() == 1) return predicateFilters.iterator().next();

        if (predicatesHolder.getClause().equals(PredicatesHolder.Clause.And)) {
            return predicateFilters.stream().reduce(Condition::and).get();
        } else if (predicatesHolder.getClause().equals(PredicatesHolder.Clause.Or)) {
            return predicateFilters.stream().reduce(Condition::or).get();
        } else throw new IllegalArgumentException("Unexpected clause in predicatesHolder: " + predicatesHolder);
    }

    private Condition extractCondition(HasContainer hasContainer) {
        String key = hasContainer.getKey();
        // has((String) null, value) is legal in Gremlin; no column has a null key, so nothing matches.
        if (key == null) return DSL.falseCondition();
        P predicate = hasContainer.getPredicate();
        Object value = predicate.getValue();

        BiPredicate<?, ?> biPredicate = predicate.getBiPredicate();

        Field<Object> field = columnField(key);
        if (predicate instanceof ConnectiveP){
            return handleConnectiveP(key, (ConnectiveP) predicate);
        }
        else if (predicate instanceof ExistsP) {
            return field.isNotNull();
        } else {
            if (idFields.contains(key) || isJsonbKey(key) || uuidColumns.contains(key)) value = stringifyValue(value);
            else if (timestamptzColumns.contains(key)) value = coerceTemporal(value);
            return predicateToQuery(field, value, biPredicate);
        }
    }

    private Condition handleConnectiveP(String key, ConnectiveP predicate) {
        Field<Object> field = columnField(key);
        List<P> predicates = predicate.getPredicates();
        List<Condition> queries = predicates.stream().map(p -> {
            if (p instanceof ConnectiveP) return handleConnectiveP(key, (ConnectiveP) p);
            Object pValue = p.getValue();
            BiPredicate pBiPredicate = p.getBiPredicate();
            return predicateToQuery(field, pValue, pBiPredicate);
        }).collect(Collectors.toList());
        Condition condition = queries.get(0);
        if (predicate instanceof AndP){
            for (int i = 1; i < queries.size(); i++) {
                condition = condition.and(queries.get(i));
            }
        }
        else if (predicate instanceof OrP){
            for (int i = 1; i < queries.size(); i++) {
                condition = condition.or(queries.get(i));
            }
        }
        else throw new IllegalArgumentException("Connective predicate not supported by unipop");
        return condition;
    }

    private Condition predicateToQuery(Field<Object> field, Object value, BiPredicate<?, ?> biPredicate) {
        if (biPredicate instanceof org.apache.tinkerpop.gremlin.process.traversal.NotP.NotPBiPredicate) {
            // P.not(inner) — translate the wrapped predicate and negate the resulting SQL condition.
            BiPredicate<?, ?> inner =
                    ((org.apache.tinkerpop.gremlin.process.traversal.NotP.NotPBiPredicate<?, ?>) biPredicate).getOriginal();
            return predicateToQuery(field, value, inner).not();
        }
        if (biPredicate instanceof Compare) {
            return getCompareCondition(value, biPredicate, field);
        } else if (biPredicate instanceof Contains) {
            Condition x = getContainsCondition(value, biPredicate, field);
            if (x != null) return x;
        }
        else if (biPredicate instanceof Text.TextPredicate) {
            return getTextCondition(value, biPredicate, field);
        } else if (biPredicate instanceof org.apache.tinkerpop.gremlin.process.traversal.Text.RegexPredicate) {
            // TinkerPop TextP.regex(...) / notRegex(...) — render as POSTGRES regex match (~ / !~).
            org.apache.tinkerpop.gremlin.process.traversal.Text.RegexPredicate rp =
                    (org.apache.tinkerpop.gremlin.process.traversal.Text.RegexPredicate) biPredicate;
            return rp.isNegate() ? field.notLikeRegex(rp.getPattern()) : field.likeRegex(rp.getPattern());
        } else if (biPredicate instanceof org.apache.tinkerpop.gremlin.process.traversal.Text) {
            return getTinkerpopTextCondition(value, (org.apache.tinkerpop.gremlin.process.traversal.Text) biPredicate, field);
        } else if (biPredicate instanceof org.apache.tinkerpop.gremlin.process.traversal.CompareType) {
            // CompareType.typeOf(GType) checks a value's runtime type. SQL columns are strongly
            // typed, so on a typed column "is of type X" reduces to "the value is present" for the
            // column's own type. Unipop configs don't declare per-property types, so this is a
            // best-effort isNotNull: correct when the queried GType matches the column's SQL type
            // (the normal case); a deliberately mismatched GType would match values it strictly
            // shouldn't. cf. enum/jsonb, which are likewise best-effort on this provider.
            return field.isNotNull();
        } else if (biPredicate instanceof Date.DatePredicate) {
            try {
                return getDateCondition(value, biPredicate, field);
            } catch (ParseException e) {
                throw new IllegalArgumentException("cant convert to date");
            }
        }
        throw new IllegalArgumentException("can't create condition for predicate "
                + (biPredicate == null ? "null" : biPredicate.getClass().getName() + " (" + biPredicate + ")"));
    }

    /**
     * Map TinkerPop's native {@link org.apache.tinkerpop.gremlin.process.traversal.Text} predicates
     * (produced by {@code TextP.containing/startingWith/endingWith} and their negations) to SQL.
     * Unipop's own {@link Text.TextPredicate} uses LIKE wildcards; TinkerPop's {@code TextP} take
     * literal substrings, so jOOQ's {@code contains}/{@code startsWith}/{@code endsWith} are used —
     * they bind the value as a parameter and escape LIKE metacharacters, avoiding injection and
     * false matches on {@code %}/{@code _} inside the search string.
     */
    private Condition getTinkerpopTextCondition(Object value, org.apache.tinkerpop.gremlin.process.traversal.Text predicate, Field<Object> field) {
        String search = value == null ? "" : value.toString();
        switch (predicate) {
            case startingWith:
                return field.startsWith(search);
            case notStartingWith:
                return field.startsWith(search).not();
            case endingWith:
                return field.endsWith(search);
            case notEndingWith:
                return field.endsWith(search).not();
            case containing:
                return field.contains(search);
            case notContaining:
                return field.contains(search).not();
            default:
                throw new IllegalArgumentException("text predicate not supported in has step: " + predicate);
        }
    }

    private Condition getDateCondition(Object value, BiPredicate<?, ?> biPredicate, Field<Object> field) throws ParseException {
        String predicateString = biPredicate.toString();
        switch (predicateString) {
            case ("eq"):
                return field.eq(convertToSqlDate(value.toString()));
            case ("neq"):
                return field.notEqual(convertToSqlDate(value.toString()));
            case ("gt"):
                return field.greaterThan(convertToSqlDate(value.toString()));
            case ("gte"):
                return field.greaterOrEqual(convertToSqlDate(value.toString()));
            case ("lt"):
                return field.lessThan(convertToSqlDate(value.toString()));
            case ("lte"):
                return field.lessOrEqual(convertToSqlDate(value.toString()));
            case ("inside"):
                List items = (List) value;
                Object firstItem = convertToSqlDate(items.get(0).toString());
                Object secondItem = convertToSqlDate(items.get(1).toString());
                return field.between(firstItem, secondItem);
            case ("within"):
                List v = (List) value;
                List<java.sql.Date> dates = new ArrayList<>();
                for (Object o : v)
                    dates.add(convertToSqlDate(o.toString()));
                return field.in(dates);
            case ("without"):
                List v2 = (List) value;
                List<java.sql.Date> dates2 = new ArrayList<>();
                for (Object o : v2)
                    dates2.add(convertToSqlDate(o.toString()));
                return field.notIn(dates2);
            default:
                throw new IllegalArgumentException("predicate not supported in has step: " + biPredicate.toString());
        }
    }

    private java.sql.Date convertToSqlDate(String dateString) throws ParseException {
        // TODO: make configurable
        long time = new MultiDateFormat("dd/MM/yyyy HH:mm:ss", Arrays.asList("dd/MM/yyyy", "yyyy-MM-dd")).parse(dateString).getTime();
        return new java.sql.Date(time);
    }

    private Condition getCompareCondition(Object value, BiPredicate<?, ?> biPredicate, Field<Object> field) {
        String predicateString = biPredicate.toString();
        switch (predicateString) {
            case ("eq"):
                return field.eq(value);
            case ("neq"):
                return field.notEqual(value);
            case ("gt"):
                return field.greaterThan(value);
            case ("gte"):
                return field.greaterOrEqual(value);
            case ("lt"):
                return field.lessThan(value);
            case ("lte"):
                return field.lessOrEqual(value);
            case ("inside"):
                List items = (List) value;
                Object firstItem = items.get(0);
                Object secondItem = items.get(1);
                return field.between(firstItem, secondItem);
            default:
                throw new IllegalArgumentException("predicate not supported in has step: " + biPredicate.toString());
        }
    }

    private Condition getContainsCondition(Object value, BiPredicate<?, ?> biPredicate, Field<Object> field) {
        if (biPredicate == Contains.without) {
            if (value == null) {
                return field.isNull();
            } else {
                return field.notIn(value);
            }
        } else if (biPredicate == Contains.within) {
            if (value == null) {
                return field.isNotNull();
            } else {
                return field.in(((Collection) value).toArray());
            }
        }
        return null;
    }

    private Condition getTextCondition(Object value, BiPredicate<?, ?> biPredicate, Field<Object> field) {
        String predicateString = biPredicate.toString();
        switch (predicateString) {
            case ("LIKE"):
                return field.like(value.toString().replace("*", "%"));
            case ("UNLIKE"):
                return field.notLike(value.toString().replace("*", "%"));
            case ("REGEXP"):
                return field.likeRegex(value.toString());
            case ("UNREGEXP"):
                return field.notLikeRegex(value.toString());
            case ("PREFIX"):
                return field.like(value.toString() + "%");
            case ("UNPREFIX"):
                return field.notLike(value.toString() + "%");
            default:
                throw new IllegalArgumentException("predicate not supported in has step: " + biPredicate.toString());
        }
    }
}