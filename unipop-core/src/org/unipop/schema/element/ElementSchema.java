package org.unipop.schema.element;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.structure.UniGraph;

import java.util.*;

public interface ElementSchema<E extends Element> {
    Collection<E> fromFields(Map<String, Object> fields);
    Map<String, Object> toFields(E element);
    Set<String> toFields(Set<String> propertyKeys);
    PredicatesHolder toPredicates(PredicatesHolder predicatesHolder);
    String getFieldByPropertyKey(String key);

    default Set<ElementSchema> getChildSchemas() { return Collections.emptySet(); }
}
