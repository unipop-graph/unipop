package org.unipop.common.property.impl;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.unipop.common.property.PropertySchema;
import org.unipop.query.predicates.PredicatesHolder;

import java.util.Collections;
import java.util.Map;

public class StaticPropertySchema implements PropertySchema {
    private final String key;
    private final String value;

    public StaticPropertySchema(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        return Collections.singletonMap(key, value);
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> prop) {
        return Collections.singletonMap(this.key, this.value);
    }

    @Override
    public PredicatesHolder toPredicates(HasContainer has) {
        return null;
    }

    @Override
    public String getKey() {
        return key;
    }

    public boolean test(P predicate) {
        return predicate.test(this.value);
    }
}
