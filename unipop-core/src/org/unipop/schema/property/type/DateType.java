package org.unipop.schema.property.type;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.unipop.process.predicate.Date;

import java.util.function.BiPredicate;

/**
 * Created by sbarzilay on 8/19/16.
 */
public class DateType implements PropertyType {
    @Override
    public String getType() {
        return "DATE";
    }

    @Override
    public <V> P<V> translate(P<V> predicate) {
        BiPredicate<V, V> biPredicate = predicate.getBiPredicate();
        if (biPredicate instanceof Compare){
            String predicateString = biPredicate.toString();
            V value = predicate.getValue();
            switch (predicateString){
                case "eq":
                    return Date.eq(value);
                case "neq":
                    return Date.neq(value);
                case "lt":
                    return Date.lt(value);
                case "gt":
                    return Date.gt(value);
                case "lte":
                    return Date.lte(value);
                case "gte":
                    return Date.gte(value);
                default:
                    throw new IllegalArgumentException("cant convert '" + predicateString +"' to DatePredicate");
            }
        } else
            throw new IllegalArgumentException("cant convert '" + biPredicate.toString() +"' to DatePredicate");
    }
}
