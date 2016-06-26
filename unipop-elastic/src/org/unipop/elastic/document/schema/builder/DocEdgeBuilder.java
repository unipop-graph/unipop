package org.unipop.elastic.document.schema.builder;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.elasticsearch.client.Client;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.schema.VertexSchema;
import org.unipop.schema.reference.ReferenceVertexSchema;
import org.unipop.schema.reference.ReferenceVertexSchemaBuilder;
import org.unipop.elastic.document.schema.DocEdgeSchema;
import org.unipop.elastic.document.schema.DocVertexSchema;
import org.unipop.structure.UniGraph;


public class DocEdgeBuilder extends DocSchemaBuilder<DocEdgeSchema> {

    private VertexSchema outVertexSchema;
    private VertexSchema inVertexSchema;

    public DocEdgeBuilder(JSONObject json, UniGraph graph) throws JSONException {
        super(json, graph);

        buildVertexSchema("outVertex", Direction.OUT);
        buildVertexSchema("inVertex", Direction.IN);
    }

    public DocEdgeBuilder(DocVertexSchema docVertexSchema, JSONObject json, UniGraph graph) throws JSONException {
        super(docVertexSchema, json, graph);

        Direction direction = Direction.valueOf(json.optString("direction"));
        addVertexSchema(docVertexSchema, direction);

        buildVertexSchema("vertex", direction.opposite());
    }

    private void buildVertexSchema(String key, Direction direction) {
        try {
            JSONObject outVertex = json.getJSONObject(key);
            ReferenceVertexSchema vertexSchema = new ReferenceVertexSchemaBuilder(outVertex, graph).build();
            addVertexSchema(vertexSchema, direction);
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
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