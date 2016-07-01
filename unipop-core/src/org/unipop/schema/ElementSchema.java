package org.unipop.schema;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.schema.property.PropertySchema;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;
import org.unipop.util.ConversionUtils;

import java.util.*;
import java.util.stream.Collectors;

public interface ElementSchema<E extends Element> {
    UniGraph getGraph();
    Collection<E> fromFields(Map<String, Object> fields);
    Map<String, Object> getProperties(Map<String, Object> source);
    Map<String, Object> toFields(E element);
    Set<String> toFields(Set<String> propertyKeys);
    PredicatesHolder toPredicates(PredicatesHolder predicatesHolder);
    default Set<ElementSchema> getChildSchemas() { return Collections.emptySet(); }
}
