package org.unipop.structure.traversalfilter;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.unipop.schema.element.ElementSchema;

/**
 * Created by sbarzilay on 9/26/16.
 */
public class DefaultTraversalFilter implements TraversalFilter{
    @Override
    public boolean filter(ElementSchema schema, Traversal traversal) {
        return true;
    }
}
