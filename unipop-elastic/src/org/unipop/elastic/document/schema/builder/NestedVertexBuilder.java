package org.unipop.elastic.document.schema.builder;

import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.elastic.common.ElasticClient;
import org.unipop.elastic.document.schema.NestedVertexSchema;
import org.unipop.schema.builder.SchemaBuilder;
import org.unipop.structure.UniGraph;

public class NestedVertexBuilder extends SchemaBuilder<NestedVertexSchema> {

    private final String path;
    private final String type;
    private final String index;

    public NestedVertexBuilder(String index, String type, String path, JSONObject json, ElasticClient client, UniGraph graph) throws JSONException {
        super(json, graph);
        this.index = index;
        this.type = type;
        this.path = path;
        client.validateNested(index, type, path);
    }

    @Override
    public NestedVertexSchema build() {
        return new NestedVertexSchema(index, type, path, propertySchemas, graph);
    }
}
