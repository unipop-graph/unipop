package org.unipop.elastic.document.schema.builder;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.elastic.common.ElasticClient;
import org.unipop.elastic.document.schema.DocEdgeSchema;
import org.unipop.elastic.document.schema.DocVertexSchema;
import org.unipop.schema.EdgeSchema;
import org.unipop.structure.UniGraph;


import static org.unipop.util.ConversionUtils.getList;

public class DocVertexBuilder extends DocBuilder<DocVertexSchema> {
    private final DocVertexSchema vertexSchema;
    private final ElasticClient client;

    public DocVertexBuilder(JSONObject json, ElasticClient client, UniGraph graph) throws JSONException {
        super(json, client, graph);
        this.client = client;
        this.vertexSchema = new DocVertexSchema(index, type, propertySchemas, graph);

        for(JSONObject edgeJson : getList(json, "edges")) {
            EdgeSchema docEdgeSchema = getEdgeSchema(edgeJson);
            vertexSchema.add(docEdgeSchema);
        }
    }

    private EdgeSchema getEdgeSchema(JSONObject edgeJson) throws JSONException {
        String path = edgeJson.optString("path", null);
        Direction direction = Direction.valueOf(edgeJson.optString("direction"));

        if(path == null) return new InnerEdgeBuilder(vertexSchema, direction, index, type, edgeJson, graph).build();
        return new NestedEdgeBuilder(vertexSchema, direction, index, type, path, edgeJson, client, graph).build();
    }

    @Override
    public DocVertexSchema build() {
        return vertexSchema;
    }
}
