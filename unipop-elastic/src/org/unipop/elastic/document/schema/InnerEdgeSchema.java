package org.unipop.elastic.document.schema;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.elastic.common.ElasticClient;
import org.unipop.schema.element.ElementSchema;
import org.unipop.schema.element.VertexSchema;
import org.unipop.structure.UniGraph;

import java.util.Set;

public class InnerEdgeSchema extends DocEdgeSchema {
    private final VertexSchema childVertexSchema;

    public InnerEdgeSchema(VertexSchema parentVertexSchema, Direction parentDirection, String index, String type, JSONObject edgeJson, ElasticClient client, UniGraph graph) throws JSONException {
        super(edgeJson, client, graph);
        this.index = index;
        this.type = type;

        this.childVertexSchema = createVertexSchema("vertex");
        this.outVertexSchema = parentDirection.equals(Direction.OUT) ? parentVertexSchema : childVertexSchema;
        this.inVertexSchema = parentDirection.equals(Direction.IN) ? parentVertexSchema : childVertexSchema;
    }

    @Override
    public Set<ElementSchema> getChildSchemas() {
        return Sets.newHashSet(childVertexSchema);
    }
}
