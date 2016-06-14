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
import org.unipop.query.predicates.PredicatesHolder;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;

import static org.jooq.impl.DSL.field;

/**
 * Created by GurRo on 6/14/2016.
 *
 * @author GurRo
 * @since 6/14/2016
 */
public class JdbcPredicatesTranslator implements PredicatesTranslator<Iterable<Condition>> {
    @Override
    public Iterable<Condition> translate(PredicatesHolder holder) {
        Set<Condition> conditions = Sets.newHashSet();

        for (HasContainer container : holder.getPredicates()) {
            Condition extractedCondition = extractCondition(container);
            if (extractedCondition == null) {
                throw new IllegalArgumentException("predicate not supported by unipop: has");
            }
            conditions.add(extractedCondition);
        }

        return conditions;
    }

    private Condition extractCondition(HasContainer hasContainer) {
        String key = hasContainer.getKey();
        Object value = hasContainer.getValue();

        BiPredicate<?, ?> predicate = hasContainer.getBiPredicate();

        if (key.equals(T.label.toString())) return DSL.trueCondition();

        if (key.equals("~id"))
            return field(T.id.toString()).in(value.getClass().isArray() ? (Object[]) value : new Object[]{value});
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
        }
        else if (predicate instanceof Contains) {
            if (predicate == Contains.without) {
                return field.isNull();
            }
            else if (predicate == Contains.within) {
                if (value == null) return field.isNotNull();
                else {
                    return field.in(((Collection) value).toArray());
                }
            }
        }
        return null;
    }
}
