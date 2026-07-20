package org.unipop.schema.element;

import org.apache.tinkerpop.gremlin.structure.Edge;

/**
 * An element schema which represents an edge
 */
public interface EdgeSchema extends ElementSchema<Edge> {

    /**
     * Out-endpoint vertex schema when the edge mapping declares one (ref or nested).
     * Default null — catalog treats the out endpoint as unresolved/open.
     */
    default VertexSchema getOutVertexSchema() {
        return null;
    }

    /**
     * In-endpoint vertex schema when the edge mapping declares one (ref or nested).
     * Default null — catalog treats the in endpoint as unresolved/open.
     */
    default VertexSchema getInVertexSchema() {
        return null;
    }
}
