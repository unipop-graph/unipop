package org.unipop.common.refer;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.common.schema.base.BaseVertexSchema;
import org.unipop.common.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.util.Map;

public class ReferringVertexSchema extends BaseVertexSchema {


    public ReferringVertexSchema(Map<String, PropertySchema> properties, UniGraph graph) {
        super(properties, null, graph);
    }

    @Override
    public Vertex fromFields(Map<String, Object> fields) {
        Map<String, Object> properties = getProperties(fields);
        return new DeferredVertex(properties, graph);
    }
}
