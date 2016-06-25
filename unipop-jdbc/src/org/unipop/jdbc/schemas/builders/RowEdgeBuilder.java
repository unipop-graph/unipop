package org.unipop.jdbc.schemas.builders;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.json.JSONObject;
import org.unipop.jdbc.schemas.RowEdgeSchema;
import org.unipop.jdbc.schemas.RowVertexSchema;
import org.unipop.schema.VertexSchema;
import org.unipop.schema.reference.ReferenceVertexSchema;
import org.unipop.schema.reference.ReferenceVertexSchemaBuilder;
import org.unipop.structure.UniGraph;

/**
 * @author Gur Ronen
 * @since 6/25/2016
 */
public class RowEdgeBuilder extends RowSchemaBuilder<RowEdgeSchema> {
    private VertexSchema outVertexSchema;
    private VertexSchema inVertexSchema;

    public RowEdgeBuilder(JSONObject json, UniGraph graph) {
        super(json, graph);

        buildVertexSchema("outVertex", Direction.OUT);
        buildVertexSchema("inVertex", Direction.IN);
    }

    public RowEdgeBuilder(RowVertexSchema rowVertexSchema,
                          JSONObject json,
                          UniGraph graph) {
        super(rowVertexSchema, json, graph);

        Direction dir = Direction.valueOf(json.optString("direction"));
        addVertexSchema(rowVertexSchema, dir);

        buildVertexSchema("vertex", dir.opposite());

    }

    @Override
    public RowEdgeSchema build() {
        return new RowEdgeSchema(
                this.outVertexSchema,
                this.inVertexSchema,
                this.propertySchemas,
                this.graph,
                this.table
        );
    }

    @SuppressWarnings("Duplicates")
    private void buildVertexSchema(String key, Direction direction) {
        try {
            JSONObject outVertex = json.getJSONObject(key);
            ReferenceVertexSchema vertexSchema = new ReferenceVertexSchemaBuilder(outVertex, graph).build();
            addVertexSchema(vertexSchema, direction);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void addVertexSchema(VertexSchema vertexSchema, Direction direction) {
        if (direction.equals(Direction.OUT)) this.outVertexSchema = vertexSchema;
        else this.inVertexSchema = vertexSchema;
    }
}

