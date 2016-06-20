package org.unipop.elastic.document.schema;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.unipop.common.property.PropertySchema;
import org.unipop.common.schema.VertexSchema;
import org.unipop.common.schema.base.BaseEdgeSchema;
import org.unipop.structure.UniGraph;

import java.util.List;

public class DocEdgeSchema extends BaseEdgeSchema implements DocSchema<Edge> {
    private String index;
    private String type;

    public DocEdgeSchema(String index, String type, VertexSchema outVertexSchema, VertexSchema inVertexSchema, List<PropertySchema> properties, UniGraph graph) {
        super(outVertexSchema, inVertexSchema, properties, graph);
        this.index = index;
        this.type = type;
    }

    @Override
    public String getIndex() {
        return this.index;
    }

    @Override
    public String getType(Edge edge) {
        if(this.type != null) return this.type;
        return edge.label();
    }
}
