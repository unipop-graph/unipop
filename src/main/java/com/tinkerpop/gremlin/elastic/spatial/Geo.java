package com.tinkerpop.gremlin.elastic.spatial;

import com.vividsolutions.jts.geom.Geometry;
import org.elasticsearch.common.Preconditions;

import java.util.function.BiPredicate;

/**
 * Created by Eliran on 6/3/2015.
 */
public enum Geo implements BiPredicate {
    /**
     * Whether the intersection between two geographic regions is non-empty
     */
    INTERSECT {

        @Override
        public String toString() {
            return "Geo.INTERSECTS";
        }


        @Override
        public boolean test(Object o, Object o2) {
            Preconditions.checkArgument(o2 instanceof Geometry);
            if (o == null) return false;
            Preconditions.checkArgument(o instanceof Geometry);
            return ((Geometry) o).crosses((Geometry) o2);

        }
    },

    /**
     * Whether the intersection between two geographic regions is empty
     */
    DISJOINT {
        @Override
        public String toString() {
            return "Geo.DISJOINT";
        }

        @Override
        public boolean test(Object o, Object o2) {
            Preconditions.checkArgument(o2 instanceof Geometry);
            if (o == null) return false;
            Preconditions.checkArgument(o instanceof Geometry);
            return ((Geometry) o).disjoint((Geometry) o2);

        }

    },

    /**
     * Whether one geographic region is completely contains within another
     */
    WITHIN {
        @Override
        public String toString() {
            return "Geo.WITHIN";
        }


        @Override
        public boolean test(Object o, Object o2) {
            Preconditions.checkArgument(o2 instanceof Geometry);
            if (o == null) return false;
            Preconditions.checkArgument(o instanceof Geometry);
            return ((Geometry) o).within((Geometry) o2);

        }
    };

}


