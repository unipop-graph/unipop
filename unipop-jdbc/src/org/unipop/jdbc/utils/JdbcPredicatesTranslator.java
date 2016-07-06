package org.unipop.jdbc.utils;

import com.google.common.collect.Sets;
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
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.jooq.impl.DSL.field;

/**
 * @author Gur Ronen
 * @since 6/14/2016
 */
public class JdbcPredicatesTranslator implements PredicatesTranslator<Condition> {

    @Override
    public Condition translate(PredicatesHolder holder) {
        Condition predicates = this.extractCondition(holder.getPredicates(), holder.getClause());

        Condition children = DSL.trueCondition();

        for (PredicatesHolder childHolder : holder.getChildren()) {
            children = appendCondition(children, translate(childHolder), holder.getClause());
        }

        return predicates.and(children);
    }

    private Condition extractCondition(Collection<HasContainer> hasContainers, PredicatesHolder.Clause clause) {
        Condition condition = DSL.trueCondition();

        for (HasContainer hasContainer : hasContainers) {
            String key = hasContainer.getKey();
            Object value = hasContainer.getValue();

            BiPredicate<?, ?> predicate = hasContainer.getBiPredicate();

            if (key.equals(T.label.toString())) return appendCondition(condition, DSL.trueCondition(), clause);

            if (key.equals("~id"))
                condition = appendCondition(condition, field(T.id.toString()).in(value.getClass().isArray() ? (Object[]) value : new Object[]{value}), clause);
            if (key.equals("~label"))
                condition = appendCondition(condition, field(T.label.toString()).in(value.getClass().isArray() ? (Object[]) value : new Object[]{value}), clause);
            Field<Object> field = field(key);
            if (predicate instanceof Compare) {
                String predicateString = predicate.toString();
                switch (predicateString) {
                    case ("eq"):
                        condition = appendCondition(condition, field.eq(value), clause);
                        break;
                    case ("neq"):
                        condition = appendCondition(condition, field.notEqual(value), clause);
                        break;
                    case ("gt"):
                        condition = appendCondition(condition, field.greaterThan(value), clause);
                        break;
                    case ("gte"):
                        condition = appendCondition(condition, field.greaterOrEqual(value), clause);
                        break;
                    case ("lt"):
                        condition = appendCondition(condition, field.lessThan(value), clause);
                        break;
                    case ("lte"):
                        condition = appendCondition(condition, field.lessOrEqual(value), clause);
                        break;
                    case ("inside"):
                        List items = (List) value;
                        Object firstItem = items.get(0);
                        Object secondItem = items.get(1);

                        condition = appendCondition(condition, field.between(firstItem, secondItem), clause);
                        break;
                    default:
                        throw new IllegalArgumentException("predicate not supported in has step: " + predicate.toString());
                }
            } else if (predicate instanceof Contains) {
                if (predicate == Contains.without) {
                    condition = appendCondition(condition, field.isNull(), clause);
                } else if (predicate == Contains.within) {
                    if (value == null) return appendCondition(condition, field.isNotNull(), clause);
                    else {
                        condition = appendCondition(condition, field.in(((Collection) value).toArray()), clause);
                    }
                }
            } else if (predicate instanceof ExistsP) {
                condition = appendCondition(condition, field.isNotNull(), clause);
            }

        }

        return condition;
    }

    private Condition appendCondition(Condition initialCondition, Condition appendingCondition, PredicatesHolder.Clause clause) {
        switch (clause) {
            case And: return initialCondition.and(appendingCondition);
            case Or: return initialCondition.or(appendingCondition);
            default: return initialCondition;
        }

    }
}
