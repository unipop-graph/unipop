package org.unipop.elastic.document.schema.builder;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.elastic.document.schema.DocEdgeSchema;
import org.unipop.elastic.document.schema.DocVertexSchema;
import org.unipop.schema.VertexSchema;
import org.unipop.schema.builder.SchemaBuilder;
import org.unipop.schema.reference.ReferenceVertexSchemaBuilder;
import org.unipop.structure.UniGraph;

public class InnerEdgeBuilder extends SchemaBuilder<DocEdgeSchema> {

    private final DocVertexSchema parentVertexSchema;
    private final Direction parentDirection;
    protected final String index;
    protected final String type;

    public InnerEdgeBuilder(DocVertexSchema parentVertexSchema, Direction parentDirection, String index, String type, JSONObject json, UniGraph graph) throws JSONException {
        super(json, graph);
        this.parentVertexSchema = parentVertexSchema;
        this.parentDirection = parentDirection;
        this.index = index;
        this.type = type;
    }

    @Override
    public DocEdgeSchema build() throws JSONException {
        JSONObject vertexJson = this.json.getJSONObject("vertex");
        VertexSchema childVertexSchema = createChildSchema(vertexJson);
        VertexSchema outVertexSchema = parentDirection.equals(Direction.OUT) ? parentVertexSchema : childVertexSchema;
        VertexSchema inVertexSchema = parentDirection.equals(Direction.IN) ? parentVertexSchema : childVertexSchema;
        return new DocEdgeSchema(index, type, outVertexSchema, inVertexSchema, propertySchemas, graph);
    }

    protected VertexSchema createChildSchema(JSONObject json) throws JSONException {
        if(json.optBoolean("ref", false)) return new ReferenceVertexSchemaBuilder(json, graph).build();
        return new InnerVertexBuilder(index, type, json, graph).build();
    }
}