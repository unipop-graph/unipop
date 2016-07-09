package org.unipop.jdbc.utils;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.unipop.common.util.PredicatesTranslator;
import org.unipop.process.predicate.ExistsP;
import org.unipop.query.predicates.PredicatesHolder;

import java.util.Collection;
import java.util.List;
import java.util.Set;
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

        if(predicateFilters.size() == 0) return DSL.trueCondition();
        if(predicateFilters.size() == 1) return predicateFilters.iterator().next();

        if(predicatesHolder.getClause().equals(PredicatesHolder.Clause.And)){
            return predicateFilters.stream().reduce(Condition::and).get();
        }
        else if(predicatesHolder.getClause().equals(PredicatesHolder.Clause.Or)){
            return predicateFilters.stream().reduce(Condition::or).get();
        }
        else throw new IllegalArgumentException("Unexpected clause in predicatesHolder: " + predicatesHolder);
    }

    private Condition extractCondition(HasContainer hasContainer) {
            String key = hasContainer.getKey();
            Object value = hasContainer.getValue();

            BiPredicate<?, ?> predicate = hasContainer.getBiPredicate();
//5

        if (key.equals("~id"))
                return field(T.id.toString()).in(value.getClass().isArray() ? (Object[]) value : new Object[]{value});
            if (key.equals("~label"))
                return field(T.label.toString()).in(value.getClass().isArray() ? (Object[]) value : new Object[]{value});
            Field<Object> field = field(key);
            if (predicate instanceof Compare) {
                String predicateString = predicate.toString();
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
                        throw new IllegalArgumentException("predicate not supported in has step: " + predicate.toString());
                }
            } else if (predicate instanceof Contains) {
                if (predicate == Contains.without) {
                    return field.isNull();
                } else if (predicate == Contains.within) {
                    if (value == null) return field.isNotNull();
                    else {
                        return field.in(((Collection) value).toArray());
                    }
                }
            } else if (predicate instanceof ExistsP) {
                return field.isNotNull();
            }

        throw new IllegalArgumentException("Predicate not supported in JDBC");
    }

    private Condition appendCondition(Condition initialCondition, Condition appendingCondition, PredicatesHolder.Clause clause) {
        switch (clause) {
            case And: return initialCondition.and(appendingCondition);
            case Or: return initialCondition.or(appendingCondition);
            default: return initialCondition;
        }

    }
}
