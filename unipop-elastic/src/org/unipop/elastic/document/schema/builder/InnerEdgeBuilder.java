package org.unipop.elastic.document.schema.builder;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.elastic.document.schema.DocEdgeSchema;
import org.unipop.elastic.document.schema.DocVertexSchema;
import org.unipop.elastic.document.schema.nested.NestedEdgeSchema;
import org.unipop.schema.VertexSchema;
import org.unipop.schema.builder.SchemaBuilder;
import org.unipop.schema.reference.ReferenceVertexSchemaBuilder;
import org.unipop.structure.UniGraph;

public class InnerEdgeBuilder extends SchemaBuilder<DocEdgeSchema> {

    protected final String index;
    protected final String type;
    protected VertexSchema outVertexSchema;
    protected VertexSchema inVertexSchema;

    public InnerEdgeBuilder(DocVertexSchema docVertexSchema, Direction direction, String index, String type, JSONObject json, UniGraph graph) throws JSONException {
        super(json, graph);

        this.index = index;
        this.type = type;

        addVertexSchema(docVertexSchema, direction);

        try {
            JSONObject vertexJson = this.json.getJSONObject("vertex");
            VertexSchema vertexSchema = createVertexSchema(vertexJson);
            addVertexSchema(vertexSchema, direction.opposite());
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    protected VertexSchema createVertexSchema(JSONObject json) throws JSONException {
        if(json.optBoolean("ref", false)) return new ReferenceVertexSchemaBuilder(json, graph).build();
        return new InnerVertexBuilder(index, type, json, graph).build();
    }

    private void addVertexSchema(VertexSchema vertexSchema, Direction direction) {
        if(direction.equals(Direction.OUT)) this.outVertexSchema = vertexSchema;
        else this.inVertexSchema = vertexSchema;
    }

    @Override
    public DocEdgeSchema build() {
        return new DocEdgeSchema(index, type, outVertexSchema, inVertexSchema, propertySchemas, graph);
    }
}
