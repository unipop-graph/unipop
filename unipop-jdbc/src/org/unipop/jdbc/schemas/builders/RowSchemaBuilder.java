package org.unipop.jdbc.schemas.builders;

import org.json.JSONObject;
import org.unipop.jdbc.schemas.RowSchema;
import org.unipop.schema.builder.SchemaBuilder;
import org.unipop.structure.UniGraph;

/**
 * @author Gur Ronen
 * @since 6/25/2016
 */
public abstract class RowSchemaBuilder<S extends RowSchema> extends SchemaBuilder<S> {
    protected String table;

    public RowSchemaBuilder(JSONObject json, UniGraph graph) {
        super(json, graph);
        this.table = json.getString("table");
    }

    public RowSchemaBuilder(RowSchema parent, JSONObject json, UniGraph graph) {
        this(json, graph);
        this.table = parent.getTable();
    }
}
