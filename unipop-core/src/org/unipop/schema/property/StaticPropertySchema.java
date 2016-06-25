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
        Object value = source.get(this.key);
        if(value != null && !this.value.equals(value)) return null;
        return Collections.singletonMap(key, this.value);
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> prop) {
        Object value = prop.get(this.key);
        if(value != null && !this.value.equals(value)) return null;
        return Collections.emptyMap();
    }

    @Override
    public PredicatesHolder toPredicates(HasContainer has) {
        if(has.getKey().equals(this.key)) {
            if(this.test(has.getPredicate())) return PredicatesHolderFactory.empty();
            else return PredicatesHolderFactory.abort();
        }
        return null;
    }

    @Override
    public Set<String> getFields() {
        return Collections.singleton(this.value);
    }

    @Override
    public Set<String> getProperties() {
        return Collections.singleton(this.key);
    }

    public boolean test(P predicate) {
        return predicate.test(this.value);
    }
}
