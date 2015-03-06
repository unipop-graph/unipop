package com.tinkerpop.gremlin.elastic.structure;

import com.vividsolutions.jts.geom.Geometry;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.geo.ShapeRelation;

import java.util.function.BiFunction;
import java.util.function.BiPredicate;

public enum Geo implements BiPredicate {
    /**
     * Whether the intersection between two geographic regions is non-empty
     */

    INTERSECT(ShapeRelation.INTERSECTS,(geometry1, geometry2)-> geometry1.intersects(geometry2)) ,

    /**
     * Whether the intersection between two geographic regions is empty
     */
    DISJOINT(ShapeRelation.DISJOINT, (geometry1, geometry2)-> geometry1.disjoint(geometry2)),

    /**
     * Whether one geographic region is completely contains within another
     */
    WITHIN(ShapeRelation.WITHIN, (geometry1, geometry2)-> geometry1.within(geometry2));

    private ShapeRelation relation;
    private BiFunction<Geometry, Geometry, Boolean> testFunc;

    Geo(ShapeRelation relation, BiFunction<Geometry,Geometry, Boolean> testFunc) {

        this.relation = relation;
        this.testFunc = testFunc;
    };

    public ShapeRelation getRelation() {
        return relation;
    }

    @Override
    public boolean test(Object o, Object o2) {
        Preconditions.checkArgument(o2 instanceof Geometry);
        if (o == null) return false;
        Preconditions.checkArgument(o instanceof Geometry);
        return testFunc.apply((Geometry)o, (Geometry)o2);
    }
}




