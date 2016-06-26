package org.unipop.schema.reference;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.schema.base.BaseVertexSchema;
import org.unipop.schema.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.util.List;
import java.util.Map;

public class ReferenceVertexSchema extends BaseVertexSchema {

    public ReferenceVertexSchema(List<PropertySchema> properties, UniGraph graph) {
        super(properties, graph);
    }

    @Override
    public Vertex fromFields(Map<String, Object> fields) {
        Map<String, Object> properties = getProperties(fields);
        if(properties == null) return null;
        return new DeferredVertex(properties, graph);
    }
}
