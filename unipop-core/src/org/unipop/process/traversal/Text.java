package org.unipop.process.traversal;

import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.function.BiPredicate;

/**
 * Created by sbarzilay on 12/15/15.
 */
public enum Text implements BiPredicate<Object, Object> {
    LIKE {
        @Override
        public boolean test(final Object first, final Object second) {
            return second.toString().matches(first.toString().replace("?", ".?").replace("*", ".*?"));
        }

        /**
         * The negative of {@code LIKE} is {@link #UNLIKE}.
         */
        @Override
        public Text negate() {
            return UNLIKE;
        }
    },
    UNLIKE {
        @Override
        public boolean test(final Object first, final Object second) {
            return !second.toString().matches(first.toString().replace("?", ".?").replace("*", ".*?"));
        }

        /**
         * The negative of {@code UNLIKE} is {@link #LIKE}.
         */
        @Override
        public Text negate() {
            return LIKE;
        }
    },
    REGEXP {
        @Override
        public boolean test(final Object first, final Object second) {
            return second.toString().matches(first.toString());
        }

        /**
         * The negative of {@code REGEXP} is {@link #UNREGEXP}.
         */
        @Override
        public Text negate() {
            return UNREGEXP;
        }
    },
    UNREGEXP {
        @Override
        public boolean test(final Object first, final Object second) {
            return !second.toString().matches(first.toString());
        }

        /**
         * The negative of {@code UNRGEXP} is {@link #REGEXP}.
         */
        @Override
        public Text negate() {
            return REGEXP;
        }
    },
    FUZZY {
        @Override
        public boolean test(final Object first, final Object second) {
            throw new NotImplementedException("Fuzzy search only implemented in elastic controllers");
        }

        /**
         * The negative of {@code FUZZY} is {@link #UNFUZZY}.
         */
        @Override
        public Text negate() {
            return UNFUZZY;
        }
    },
    UNFUZZY {
        @Override
        public boolean test(final Object first, final Object second) {
            throw new NotImplementedException("Fuzzy search only implemented in elastic controllers");
        }

        /**
         * The negative of {@code UNFUZZY} is {@link #FUZZY}.
         */
        @Override
        public Text negate() {
            return FUZZY;
        }
    };

    public static <V> P<V> like(final V value) { return new P(Text.LIKE, value); }
    public static <V> P<V> unlike(final V value) { return new P(Text.UNLIKE, value); }
    public static <V> P<V> regexp(final V value) { return new P(Text.REGEXP, value); }
    public static <V> P<V> unregexp(final V value) { return new P(Text.UNREGEXP, value); }
    public static <V> P<V> fuzzy(final V value) { return new P(Text.FUZZY, value); }
    public static <V> P<V> unfuzzy(final V value) { return new P(Text.UNFUZZY, value); }
}
