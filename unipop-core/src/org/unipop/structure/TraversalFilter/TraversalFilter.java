package org.unipop.structure.TraversalFilter;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.unipop.schema.element.ElementSchema;

public interface TraversalFilter {
    boolean filter(ElementSchema schema, Traversal traversal);
}
