package org.unipop.common.schema.property;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.javatuples.Pair;

import java.util.Iterator;
import java.util.Map;

public interface PropertySchema {

    Pair<String, Object> toProperty(Map<String, Object> source) ;

    Iterator<Pair<String, Object>> toFields(Object prop);

    default boolean test(P predicate){
        return true;
    }
}
