package org.unipop.schema;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.query.predicates.PredicatesHolder;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ElementSchema<E extends Element> {
    E fromFields(Map<String, Object> fields);
    Map<String, Object> toFields(E element);
    Set<String> toFields(Set<String> propertyKeys);
    PredicatesHolder toPredicates(PredicatesHolder predicates);
    default Set<ElementSchema> getAllSchemas() {
        return Sets.newHashSet(this);
    }
}
