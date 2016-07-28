package org.unipop.jdbc.schemas;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.jooq.Field;
import org.jooq.Query;
import org.jooq.impl.DSL;
import org.json.JSONObject;
import org.unipop.jdbc.schemas.jdbc.JdbcSchema;
import org.unipop.jdbc.schemas.jdbc.JdbcVertexSchema;
import org.unipop.schema.element.ElementSchema;
import org.unipop.schema.element.VertexSchema;
import org.unipop.structure.UniGraph;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

/**
 * @author Gur Ronen
 * @since 7/5/2016
 */
public class InnerRowEdgeSchema extends RowEdgeSchema {
    private final JdbcVertexSchema parentVertexSchema;
    private final VertexSchema childVertexSchema;


    public InnerRowEdgeSchema(JdbcVertexSchema parentVertexSchema, Direction parentDirection, JSONObject edgeJson, String table, UniGraph graph) {
        super(edgeJson, graph);
        this.table = table;
        this.parentVertexSchema = parentVertexSchema;
        this.childVertexSchema = createVertexSchema("vertex");

        this.outVertexSchema = parentDirection.equals(Direction.OUT) ? parentVertexSchema : childVertexSchema;
        this.inVertexSchema = parentDirection.equals(Direction.IN) ? parentVertexSchema : childVertexSchema;
    }


    @Override
    public Set<ElementSchema> getChildSchemas() {
        return Sets.newHashSet(childVertexSchema);
    }

    @Override
    public String getTable() {
        return parentVertexSchema.getTable();
    }

    @Override
    public String toString() {
        return "InnerRowEdgeSchema{" +
                "childVertexSchema=" + childVertexSchema +
                "} " + super.toString();
    }

    @Override
    public Query getInsertStatement(Edge element) {
        JdbcSchema.Row row = toRow(element);
        if (row == null) return null;
        Map<Field<Object>, Object> fields = row.getFields().entrySet().stream()
                .collect(Collectors.toMap((entry) -> field(entry.getKey()), Map.Entry::getValue));

        return DSL.update(table(getTable())).set(fields).where(field(row.getIdField()).eq(row.getId()));
    }
}
