package org.unipop.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.function.BiPredicate;

/**
 * Created by sbarzilay on 12/15/15.
 */
public enum Uni implements BiPredicate<Object, Object> {
    LIKE {
        @Override
        public boolean test(final Object first, final Object second) {
            return second.toString().matches(first.toString().replace("?", ".?").replace("*", ".*?"));
        }

        /**
         * The negative of {@code LIKE} is {@link #UNLIKE}.
         */
        @Override
        public Uni negate() {
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
        public Uni negate() {
            return LIKE;
        }
    };

    public static <V> P<V> like(final V value) { return new P(Uni.LIKE, value); }
}
