package org.unipop.common.schema;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolder;

import java.util.Map;
import java.util.Set;

public interface ElementSchema<E extends Element> {
    E fromFields(Map<String, Object> fields);
    Map<String, Object> toFields(E element);
    PredicatesHolder toPredicates(PredicatesHolder predicates);
    Set<String> getIdsAndLabelsKeys();
}
