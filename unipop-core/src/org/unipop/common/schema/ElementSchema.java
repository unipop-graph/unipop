package org.unipop.common.schema;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.Iterator;
import java.util.Map;

public interface ElementSchema<E extends Element> {
    E fromFields(Map<String, Object> fields);
    Map<String, Object> toFields(E element);
}
