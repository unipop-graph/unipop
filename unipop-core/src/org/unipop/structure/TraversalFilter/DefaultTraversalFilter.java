package org.unipop.structure.TraversalFilter;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.unipop.schema.element.ElementSchema;

public class DefaultTraversalFilter implements TraversalFilter{
    @Override
    public boolean filter(ElementSchema schema, Traversal traversal) {
        return true;
    }
}
