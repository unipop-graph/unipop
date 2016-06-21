package org.unipop.common.schema.reference;

import org.json.JSONObject;
import org.unipop.common.schema.builder.SchemaBuilder;
import org.unipop.structure.UniGraph;

public class ReferenceVertexSchemaBuilder extends SchemaBuilder<ReferenceVertexSchema> {
    public ReferenceVertexSchemaBuilder(JSONObject json, UniGraph graph) {
        super(json, graph);
    }

    @Override
    public ReferenceVertexSchema build() {
        return new ReferenceVertexSchema(propertySchemas, graph);
    }
}
