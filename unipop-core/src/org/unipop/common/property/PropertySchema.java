package org.unipop.common.property;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.javatuples.Pair;

import java.util.Iterator;
import java.util.Map;

public interface PropertySchema {
    Map<String, Object> toProperties(Map<String, Object> source) ;
    Map<String, Object> toFields(Map<String, Object> properties);
    default boolean test(P predicate){
        return true;
    }
}
