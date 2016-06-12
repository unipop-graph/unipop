package org.unipop.jdbc.controller.schemas;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.common.property.PropertySchema;
import org.unipop.common.schema.base.BaseVertexSchema;
import org.unipop.structure.UniGraph;

import java.util.List;

/**
 * Created by GurRo on 6/12/2016.
 */
public class RowVertexSchema extends BaseVertexSchema implements RowSchema<Vertex> {
    private final String database;
    private final String table;

    public RowVertexSchema(List<PropertySchema> properties, UniGraph graph, String database, String table) {
        super(properties, graph);

        this.database = database;
        this.table = table;
    }

    @Override
    public String getDatabase(Vertex element) {
        return this.database;
    }

    @Override
    public String getTable(Vertex element) {
        return this.table == null ? element.label() : this.table;
    }
}
