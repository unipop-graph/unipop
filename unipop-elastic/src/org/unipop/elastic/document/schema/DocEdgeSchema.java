package org.unipop.elastic.document.schema;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.common.schema.BaseEdgeSchema;
import org.unipop.common.schema.ElementSchema;
import org.unipop.common.schema.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.util.Map;

public class DocEdgeSchema extends BaseEdgeSchema implements DocSchema<Edge> {
    private String index;
    private String type;

    public DocEdgeSchema(String index, String type, ElementSchema<Vertex> outVertexSchema, ElementSchema<Vertex> inVertexSchema, Map<String, PropertySchema> properties, boolean dynamicProperties, UniGraph graph) {
        super(outVertexSchema, inVertexSchema, properties, dynamicProperties, graph);
        this.index = index;
        this.type = type;
    }

    @Override
    public String getIndex() {
        return this.index;
    }

    @Override
    public String getType() {
        return this.type;
    }
}
