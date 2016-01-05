package org.unipop.elastic2.controller.schema.helpers.elementConverters;

import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * Created by Roman on 3/16/2015.
 */
public interface ElementConverter<TElementSource extends Element, TElementDest extends Element> {
    boolean canConvert(TElementSource source);
    Iterable<TElementDest> convert(TElementSource source);
}
