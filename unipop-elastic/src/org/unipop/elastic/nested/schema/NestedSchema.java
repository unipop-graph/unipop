package org.unipop.elastic.nested.schema;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.elastic.document.schema.DocSchema;

public interface NestedSchema<E extends Element> extends DocSchema<E> {
    String getPath();
}
