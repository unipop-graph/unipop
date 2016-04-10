package org.unipop.common.refer;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.common.schema.base.BaseVertexSchema;
import org.unipop.common.schema.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.util.Map;

public class RefferingVertexSchema extends BaseVertexSchema {


    public RefferingVertexSchema(Map<String, PropertySchema> properties, UniGraph graph) {
        super(properties, false, graph);
    }

    @Override
    public Vertex fromFields(Map<String, Object> fields) {
        Map<String, Object> properties = getProperties(fields);
        return new DeferredVertex(properties, graph);
    }
}
