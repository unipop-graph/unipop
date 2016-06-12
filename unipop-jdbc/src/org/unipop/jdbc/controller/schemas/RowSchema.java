package org.unipop.jdbc.controller.schemas;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.common.schema.ElementSchema;

/**
 * Created by GurRo on 6/12/2016.
 */
public interface RowSchema<E extends Element> extends ElementSchema<E> {
    String getDatabase(E element);

    default String getTable(E element) {
        return element.label();
    }

    default Object getId(E element) {
        return element.id();
    }
}
