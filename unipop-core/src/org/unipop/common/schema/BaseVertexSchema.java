package org.unipop.common.schema;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.common.schema.property.PropertySchema;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.util.Map;

public class BaseVertexSchema extends BaseElementSchema<Vertex> {
    public BaseVertexSchema(Map<String, PropertySchema> properties, boolean dynamicProperties, UniGraph graph) {
        super(properties, dynamicProperties, graph);
    }

    @Override
    public Vertex fromFields(Map fields) {
        Map properties = getProperties(fields);
        return new UniVertex(properties, graph);
    }
}
