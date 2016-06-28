package org.unipop.schema.property;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class StaticPropertySchema implements PropertySchema {
    private final String key;
    private final String value;

    public StaticPropertySchema(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        return Collections.singletonMap(key, this.value);
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> prop) {
        Object o = prop.get(this.key);
        if(o == null || o.equals(this.value)) return Collections.emptyMap();
        return null;
    }

    @Override
    public Set<String> toFields(Set<String> propertyKeys) {
        return Collections.emptySet();
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        HasContainer has = predicatesHolder.findKey(this.key);
        if(has != null && !test(has.getPredicate())) return PredicatesHolderFactory.abort();
        return PredicatesHolderFactory.empty();
    }

    @Override
    public Set<String> excludeDynamicProperties() {
        return Collections.singleton(this.key);
    }

    private boolean test(P predicate) {
        return predicate.test(this.value);
    }
}
