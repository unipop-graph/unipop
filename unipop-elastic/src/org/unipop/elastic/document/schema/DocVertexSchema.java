package org.unipop.elastic.document.schema;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.schema.base.BaseVertexSchema;
import org.unipop.schema.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.util.List;

public class DocVertexSchema extends BaseVertexSchema implements DocSchema<Vertex> {
    private String index;
    private String type;

    public DocVertexSchema(String index, String type, List<PropertySchema> properties, UniGraph graph) {
        super(properties, graph);
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
