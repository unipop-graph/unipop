package org.unipop.jdbc.schemas.builders;

import org.json.JSONObject;
import org.unipop.common.util.ConversionUtils;
import org.unipop.jdbc.schemas.RowVertexSchema;
import org.unipop.structure.UniGraph;

/**
 * @author Gur Ronen
 * @since 6/25/2016
 */
public class RowVertexBuilder extends RowSchemaBuilder<RowVertexSchema> {
    public RowVertexBuilder(JSONObject json, UniGraph graph) {
        super(json, graph);
    }

    @Override
    public RowVertexSchema build() {
        RowVertexSchema schema = new RowVertexSchema(
                this.propertySchemas,
                this.graph,
                this.table
        );

        ConversionUtils.getList(this.json, "edges").forEach(edgeJson -> {
            RowEdgeBuilder rowEdgeBuilder = new RowEdgeBuilder(schema, edgeJson, graph);
            schema.add(rowEdgeBuilder.build());
        });

        return schema;
    }
}
