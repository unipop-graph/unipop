package org.unipop.jdbc.schemas;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.json.JSONObject;
import org.unipop.jdbc.schemas.jdbc.JdbcVertexSchema;
import org.unipop.schema.element.ElementSchema;
import org.unipop.schema.element.VertexSchema;
import org.unipop.structure.UniGraph;

import java.util.Set;

/**
 * @author Gur Ronen
 * @since 7/5/2016
 */
public class InnerRowEdgeSchema extends RowEdgeSchema {
    private final JdbcVertexSchema parentVertexSchema;
    private final VertexSchema childVertexSchema;


    public InnerRowEdgeSchema(JdbcVertexSchema parentVertexSchema, Direction parentDirection, JSONObject edgeJson, UniGraph graph) {
        super(edgeJson, graph);
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
}
