package org.unipop.elastic2.controller.schema.helpers.elementConverters.utils;

import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * Created by Roman on 3/25/2015.
 */
public interface ElementFactory<TInput, TElementOutput extends Element> {
    TElementOutput getElement(TInput input);
}
