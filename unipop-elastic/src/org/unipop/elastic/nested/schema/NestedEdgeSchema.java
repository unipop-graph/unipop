package org.unipop.elastic.nested.schema;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.unipop.common.schema.VertexSchema;
import org.unipop.common.schema.base.BaseEdgeSchema;
import org.unipop.common.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.util.Map;

public class NestedEdgeSchema extends BaseEdgeSchema implements NestedSchema<Edge> {
    private String path;
    private String index;
    private String type;

    public NestedEdgeSchema(UniGraph graph, String path, String index, String type, VertexSchema outVertexSchema, VertexSchema inVertexSchema, Map<String, PropertySchema> properties, PropertySchema dynamicProperties) {
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
