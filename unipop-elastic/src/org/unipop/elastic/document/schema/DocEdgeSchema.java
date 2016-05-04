package org.unipop.elastic.document.schema;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.unipop.common.schema.base.BaseEdgeSchema;
import org.unipop.common.schema.VertexSchema;
import org.unipop.common.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.util.Map;

public class DocEdgeSchema extends BaseEdgeSchema implements DocSchema<Edge> {
    private String index;
    private String type;

    public DocEdgeSchema(String index, String type, VertexSchema outVertexSchema, VertexSchema inVertexSchema, Map<String, PropertySchema> properties, PropertySchema dynamicProperties, UniGraph graph) {
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
