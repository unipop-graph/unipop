package org.unipop.schema.property;

import org.unipop.query.predicates.PredicatesHolder;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public interface PropertySchema {
    Map<String, Object> toProperties(Map<String, Object> source) ;
    Map<String, Object> toFields(Map<String, Object> properties);
    Set<String> toFields(Set<String> propertyKeys);
    PredicatesHolder toPredicates(PredicatesHolder predicatesHolder);

    default Set<String> excludeDynamicFields() { return Collections.emptySet(); }
    default Set<String> excludeDynamicProperties() { return Collections.emptySet(); }
}
