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
import org.jooq.impl.DSL;
import org.unipop.common.util.PredicatesTranslator;
import org.unipop.process.predicate.Date;
import org.unipop.process.predicate.ExistsP;
import org.unipop.process.predicate.Text;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.util.MultiDateFormat;

import java.text.ParseException;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import static org.jooq.impl.DSL.field;

/**
 * @author Gur Ronen
 * @since 6/14/2016
 */
public class JdbcPredicatesTranslator implements PredicatesTranslator<Condition> {

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
        P predicate = hasContainer.getPredicate();
        Object value = predicate.getValue();

        BiPredicate<?, ?> biPredicate = predicate.getBiPredicate();

        Field<Object> field = field(key);
        if (predicate instanceof ConnectiveP){
            return handleConnectiveP(key, (ConnectiveP) predicate);
        }
        else if (predicate instanceof ExistsP) {
            return field.isNotNull();
        } else return predicateToQuery(key, value, biPredicate);
    }

    private Condition handleConnectiveP(String key, ConnectiveP predicate) {
        List<P> predicates = predicate.getPredicates();
        List<Condition> queries = predicates.stream().map(p -> {
            if (p instanceof ConnectiveP) return handleConnectiveP(key, (ConnectiveP) p);
            Object pValue = p.getValue();
            BiPredicate pBiPredicate = p.getBiPredicate();
            return predicateToQuery(key, pValue, pBiPredicate);
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

    private Condition predicateToQuery(String field, Object value, BiPredicate<?, ?> biPredicate) {
        if (biPredicate instanceof Compare) {
            return getCompareCondition(value, biPredicate, field(field));
        } else if (biPredicate instanceof Contains) {
            Condition x = getContainsCondition(value, biPredicate, field(field));
            if (x != null) return x;
        }
        else if (biPredicate instanceof Text.TextPredicate) {
            return getTextCondition(value, biPredicate, field(field));
        } else if (biPredicate instanceof Date.DatePredicate) {
            try {
                return getDateCondition(value, biPredicate, field(field));
            } catch (ParseException e) {
                throw new IllegalArgumentException("cant convert to date");
            }
        }
        throw new IllegalArgumentException("can't create condition");
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