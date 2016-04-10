package org.unipop.elastic.nested.schema;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.common.schema.BaseEdgeSchema;
import org.unipop.common.schema.ElementSchema;
import org.unipop.common.schema.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.util.Map;

public class NestedEdgeSchema extends BaseEdgeSchema implements NestedSchema<Edge> {
    private String path;
    private String index;
    private String type;

    public NestedEdgeSchema(UniGraph graph, String path, String index, String type, ElementSchema<Vertex> outVertexSchema, ElementSchema<Vertex> inVertexSchema, Map<String, PropertySchema> properties, boolean dynamicProperties) {
        super(outVertexSchema, inVertexSchema, properties, dynamicProperties, graph);
        this.path = path;
        this.index = index;
        this.type = type;
    }

    @Override
    public String getPath() {
        return this.path;
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
