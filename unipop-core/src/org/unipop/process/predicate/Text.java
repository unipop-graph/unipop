package org.unipop.process.predicate;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.function.BiPredicate;

/**
 * Created by sbarzilay on 12/15/15.
 */
public class Text {
    public static <V> P<V> like(final V value) { return new P(TextPredicate.LIKE, value); }
    public static <V> P<V> unlike(final V value) { return new P(TextPredicate.UNLIKE, value); }
    public static <V> P<V> regexp(final V value) { return new P(TextPredicate.REGEXP, value); }
    public static <V> P<V> unregexp(final V value) { return new P(TextPredicate.UNREGEXP, value); }
    public static <V> P<V> fuzzy(final V value) { return new P(TextPredicate.FUZZY, value); }
    public static <V> P<V> unfuzzy(final V value) { return new P(TextPredicate.UNFUZZY, value); }
    public static <V> P<V> prefix(final V value) { return new P(TextPredicate.PREFIX, value); }
    public static <V> P<V> unprefix(final V value) { return new P(TextPredicate.UNPREFIX, value); }

    public enum TextPredicate implements BiPredicate<Object, Object> {
        PREFIX {
            @Override
            public boolean test(final Object first, final Object second) {
                return first.toString().startsWith(second.toString());
            }

            /**
             * The negative of {@code LIKE} is {@link #UNLIKE}.
             */
            @Override
            public TextPredicate negate() {
                return UNREGEXP;
            }
        },
        UNPREFIX {
            @Override
            public boolean test(final Object first, final Object second) {
                return !negate().test(first, second);
            }

            /**
             * The negative of {@code LIKE} is {@link #UNLIKE}.
             */
            @Override
            public TextPredicate negate() {
                return PREFIX;
            }
        },
        LIKE {
            @Override
            public boolean test(final Object first, final Object second) {
                return first.toString().matches(second.toString().replace("?", ".?").replace("*", ".*?"));
            }

            /**
             * The negative of {@code LIKE} is {@link #UNLIKE}.
             */
            @Override
            public TextPredicate negate() {
                return UNLIKE;
            }
        },
        UNLIKE {
            @Override
            public boolean test(final Object first, final Object second) {
                return !negate().test(first, second);
            }

            /**
             * The negative of {@code UNLIKE} is {@link #LIKE}.
             */
            @Override
            public TextPredicate negate() {
                return LIKE;
            }
        },
        REGEXP {
            @Override
            public boolean test(final Object first, final Object second) {
                return first.toString().matches(second.toString());
            }

            /**
             * The negative of {@code REGEXP} is {@link #UNREGEXP}.
             */
            @Override
            public TextPredicate negate() {
                return UNREGEXP;
            }
        },
        UNREGEXP {
            @Override
            public boolean test(final Object first, final Object second) {
                return !negate().test(first, second);
            }

            /**
             * The negative of {@code UNRGEXP} is {@link #REGEXP}.
             */
            @Override
            public TextPredicate negate() {
                return REGEXP;
            }
        },
        FUZZY {
            @Override
            public boolean test(final Object first, final Object second) {
                int levenshteinDistance = StringUtils.getLevenshteinDistance(second.toString(), first.toString());
                if (levenshteinDistance <= 3)
                    return true;
                return false;
            }

            /**
             * The negative of {@code FUZZY} is {@link #UNFUZZY}.
             */
            @Override
            public TextPredicate negate() {
                return UNFUZZY;
            }
        },
        UNFUZZY {
            @Override
            public boolean test(final Object first, final Object second) {
                return !negate().test(first, second);
            }

            /**
             * The negative of {@code UNFUZZY} is {@link #FUZZY}.
             */
            @Override
            public TextPredicate negate() {
                return FUZZY;
            }
        };
    }
}
