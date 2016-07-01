package org.unipop.elastic.document.schema.builder;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.elastic.common.ElasticClient;
import org.unipop.elastic.document.schema.DocVertexSchema;
import org.unipop.elastic.document.schema.NestedEdgeSchema;
import org.unipop.schema.VertexSchema;
import org.unipop.schema.builder.SchemaBuilder;
import org.unipop.schema.reference.ReferenceVertexSchemaBuilder;
import org.unipop.structure.UniGraph;

public class NestedEdgeBuilder extends SchemaBuilder<NestedEdgeSchema> {
    private final DocVertexSchema parentVertexSchema;
    private final Direction parentDirection;
    private final String index;
    private final String type;
    private final String path;
    private final ElasticClient client;

    public NestedEdgeBuilder(DocVertexSchema parentVertexSchema, Direction parentDirection, String index, String type, String path, JSONObject json, ElasticClient client, UniGraph graph) throws JSONException {
        super(json, graph);
        this.parentVertexSchema = parentVertexSchema;
        this.parentDirection = parentDirection;
        this.index = index;
        this.type = type;
        this.path = path;
        this.client = client;

        client.validateNested(index, type, path);
    }

    @Override
    public NestedEdgeSchema build() throws JSONException {
        JSONObject vertexJson = this.json.getJSONObject("vertex");
        VertexSchema childVertexSchema = new ReferenceVertexSchemaBuilder(vertexJson, graph).build();
        return new NestedEdgeSchema(index, type, path, parentVertexSchema, childVertexSchema, parentDirection, propertySchemas, graph);
    }
}
