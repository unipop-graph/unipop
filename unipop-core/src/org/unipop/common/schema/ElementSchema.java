package org.unipop.common.schema;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.List;
import java.util.Map;

public interface ElementSchema<E extends Element> {
    E fromFields(Map<String, Object> fields);
    Map<String, Object> toFields(E element);
    List<HasContainer> toPredicates(List<HasContainer> predicates);
}
