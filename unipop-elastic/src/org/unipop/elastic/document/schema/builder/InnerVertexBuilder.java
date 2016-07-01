package org.unipop.elastic.document.schema.builder;

import org.json.JSONObject;
import org.unipop.elastic.document.schema.DocVertexSchema;
import org.unipop.schema.builder.SchemaBuilder;
import org.unipop.structure.UniGraph;

public class InnerVertexBuilder extends SchemaBuilder<DocVertexSchema> {
    private final String index;
    private final String type;

    public InnerVertexBuilder(String index, String type, JSONObject json, UniGraph graph) {
        super(json, graph);
        this.index = index;
        this.type = type;
    }

    @Override
    public DocVertexSchema build() {
        return new DocVertexSchema(index, type, propertySchemas, graph);
    }
}
