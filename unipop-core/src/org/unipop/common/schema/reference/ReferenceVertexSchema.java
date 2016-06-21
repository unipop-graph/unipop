package org.unipop.common.schema.reference;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.common.schema.base.BaseVertexSchema;
import org.unipop.common.schema.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.util.List;
import java.util.Map;

public class ReferenceVertexSchema extends BaseVertexSchema {

    public ReferenceVertexSchema(List<PropertySchema> properties, UniGraph graph) {
        super(properties, null, graph);
    }

    @Override
    public Vertex fromFields(Map<String, Object> fields) {
        Map<String, Object> properties = getProperties(fields);
        return new DeferredVertex(properties, graph);
    }
}
