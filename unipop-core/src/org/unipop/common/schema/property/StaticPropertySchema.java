package org.unipop.common.schema.property;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.javatuples.Pair;

import java.util.Iterator;
import java.util.Map;

public class StaticPropertySchema implements PropertySchema {
    private final String key;
    private final String value;

    public StaticPropertySchema(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public Pair<String, Object> toProperty(Map<String, Object> source) {
        return new Pair<>(key, value);
    }

    @Override
    public Iterator<Pair<String, Object>> toFields(Object prop) {
        return null;
    }

    @Override
    public boolean test(P predicate) {
        return predicate.test(this.value);
    }
}
