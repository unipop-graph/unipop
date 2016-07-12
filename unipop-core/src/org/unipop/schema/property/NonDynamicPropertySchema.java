package org.unipop.schema.property;

import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class NonDynamicPropertySchema extends DynamicPropertySchema {
    public NonDynamicPropertySchema(ArrayList<PropertySchema> otherSchemas) {
        super(otherSchemas);
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> properties) {
        return Collections.emptyMap();
    }

    @Override
    public Set<String> toFields(Set<String> propertyKeys) {
        return Collections.emptySet();
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        PredicatesHolder newPredicatesHolder = super.toPredicates(predicatesHolder);
        if(newPredicatesHolder.notEmpty()) return PredicatesHolderFactory.abort();
        return newPredicatesHolder;
    }
}
