package org.unipop.structure.traversalfilter;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.unipop.schema.element.ElementSchema;


/**
 * Created by sbarzilay on 9/26/16.
 */
public interface TraversalFilter {
    boolean filter(ElementSchema schema, Traversal traversal);
}
