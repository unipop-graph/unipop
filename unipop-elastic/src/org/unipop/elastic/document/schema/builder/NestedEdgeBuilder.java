package org.unipop.elastic.document.schema.builder;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.elastic.document.schema.DocEdgeSchema;
import org.unipop.elastic.document.schema.DocVertexSchema;
import org.unipop.elastic.document.schema.nested.NestedEdgeSchema;
import org.unipop.schema.VertexSchema;
import org.unipop.structure.UniGraph;

public class NestedEdgeBuilder extends InnerEdgeBuilder {
    private final String path;

    public NestedEdgeBuilder(DocVertexSchema vertexSchema, Direction direction, String index, String type, String path, JSONObject json, UniGraph graph) throws JSONException {
        super(vertexSchema, direction, index, type, json, graph);
        this.path = path;
    }


    protected VertexSchema createVertexSchema(JSONObject json) throws JSONException {
        return new NestedVertexBuilder(index, type, path, json, graph).build();
    }

    @Override
    public DocEdgeSchema build() {
        return new NestedEdgeSchema(index, type, path, outVertexSchema, inVertexSchema, propertySchemas, graph);
    }
}
