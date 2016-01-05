package org.unipop.elastic2.controller.schema.helpers.schemaProviders;

import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * Created by Roman on 1/16/2015.
 */
public interface GraphVertexSchema extends GraphElementSchema {
    default public Class getSchemaElementType() {
        return Vertex.class;
    }
}
