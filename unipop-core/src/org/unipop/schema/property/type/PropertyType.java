package org.unipop.schema.property.type;


import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.function.BiPredicate;

/**
 * Created by sbarzilay on 8/18/16.
 */

public interface PropertyType {
    String getType();
    Object convertToType(Object object);
    default <V> P<V> translate(P<V> predicate){
        return predicate;
    }
}
