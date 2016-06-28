package org.unipop.elastic.document.schema.builder;

import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.elastic.document.schema.DocVertexSchema;
import org.unipop.structure.UniGraph;


import static org.unipop.util.ConversionUtils.getList;

public class DocVertexBuilder extends DocSchemaBuilder<DocVertexSchema> {
    private final DocVertexSchema schema;

    public DocVertexBuilder(JSONObject json, UniGraph graph) throws JSONException {
        super(json, graph);
        this.schema = new DocVertexSchema(index, type, propertySchemas, graph);

        for(JSONObject edgeJson : getList(json, "edges")) {
            DocEdgeBuilder docEdgeBuilder = new DocEdgeBuilder(schema, edgeJson, graph);
            schema.add(docEdgeBuilder.build());
        }
    }

    @Override
    public DocVertexSchema build() {
        return schema;
    }
}
