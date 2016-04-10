package org.unipop.elastic.document.schema;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.common.schema.ElementSchema;

public interface DocSchema<E extends Element> extends ElementSchema<E> {
    String getIndex();
    String getType();
}
