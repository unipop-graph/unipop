package org.unipop.elastic.document.schema;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.common.schema.base.BaseVertexSchema;
import org.unipop.common.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.util.Map;

public class DocVertexSchema extends BaseVertexSchema implements DocSchema<Vertex> {
    private String index;
    private String type;

    public DocVertexSchema(String index, String type, Map<String, PropertySchema> properties, boolean dynamicProperties, UniGraph graph) {
        super(properties, dynamicProperties, graph);
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
