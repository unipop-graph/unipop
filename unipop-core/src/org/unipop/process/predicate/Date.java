package org.unipop.process.predicate;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;

import java.util.Collection;
import java.util.Arrays;
import java.util.function.BiPredicate;

/**
 * Created by sbarzilay on 8/18/16.
 */
public class Date {
    public static <V> P<V> within(final Collection<V> value) { return new P(DatePredicate.within, value);}
    public static <V> P<V> without(final Collection<V> value) { return new P(DatePredicate.without, value);}
    public static <V> P<V> eq(final V value) { return new P(DatePredicate.eq, value); }
    public static <V> P<V> neq(final V value) { return new P(DatePredicate.neq, value); }
    public static <V> P<V> lt(final V value) { return new P(DatePredicate.lt, value); }
    public static <V> P<V> gt(final V value) { return new P(DatePredicate.gt, value); }
    public static <V> P<V> gte(final V value) { return new P(DatePredicate.gte, value); }
    public static <V> P<V> lte(final V value) { return new P(DatePredicate.lte, value); }
    public static <V> P<V> inside(final V first, final V second) {
        return new AndP<>(Arrays.asList(new P(DatePredicate.gt, first), new P(DatePredicate.lt, second)));
    }
    public static <V> P<V> outside(final V first, final V second) {
        return new OrP<>(Arrays.asList(new P(DatePredicate.lt, first), new P(DatePredicate.gt, second)));
    }
    public static <V> P<V> between(final V first, final V second) {
        return new AndP<>(Arrays.asList(new P(DatePredicate.gte, first), new P(DatePredicate.lt, second)));
    }

    public enum DatePredicate implements BiPredicate<Object, Object> {
        eq {
            @Override
            public boolean test(Object o, Object o2) {
                return ((java.util.Date)o).compareTo((java.util.Date) o2) == 0;
            }

            @Override
            public BiPredicate<Object, Object> negate() {
                return neq;
            }
        },
        neq {
            @Override
            public boolean test(Object o, Object o2) {
                return !negate().test(o, o2);
            }

            @Override
            public BiPredicate<Object, Object> negate() {
                return eq;
            }
        },
        gt {
            @Override
            public boolean test(Object o, Object o2) {
                return ((java.util.Date)o).compareTo((java.util.Date) o2) > 0;
            }

            @Override
            public BiPredicate<Object, Object> negate() {
                return lte;
            }
        },
        lt {
            @Override
            public boolean test(Object o, Object o2) {
                return ((java.util.Date)o).compareTo((java.util.Date) o2) < 0;
            }

            @Override
            public BiPredicate<Object, Object> negate() {
                return gte;
            }
        },
        gte {
            @Override
            public boolean test(Object o, Object o2) {
                return gt.or(eq).test(o, o2);
            }

            @Override
            public BiPredicate<Object, Object> negate() {
                return lt;
            }
        },
        lte {
            @Override
            public boolean test(Object o, Object o2) {
                return lt.or(eq).test(o, o2);
            }

            @Override
            public BiPredicate<Object, Object> negate() {
                return gt;
            }
        },
        within {
            @Override
            public boolean test(Object o, Object o2) {
                return ((Collection) o2).contains(o);
            }

            @Override
            public BiPredicate<Object, Object> negate() {
                return without;
            }
        },
        without {
            @Override
            public boolean test(Object o, Object o2) {
                return !negate().test(o, o2);
            }

            @Override
            public BiPredicate<Object, Object> negate() {
                return within;
            }
        }
    }
}
