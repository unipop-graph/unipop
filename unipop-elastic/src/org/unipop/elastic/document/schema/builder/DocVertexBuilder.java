package org.unipop.elastic.document.schema.builder;

import org.json.JSONObject;
import org.unipop.elastic.document.schema.DocVertexSchema;
import org.unipop.structure.UniGraph;


import static org.unipop.common.util.ConversionUtils.getList;

public class DocVertexBuilder extends DocSchemaBuilder<DocVertexSchema> {

    public DocVertexBuilder(JSONObject json, UniGraph graph) {
        super(json, graph);
    }

    @Override
    public DocVertexSchema build() {
        DocVertexSchema schema = new DocVertexSchema(index, type, propertySchemas, graph);

        getList(json, "edges").forEach(edgeJson -> {
            DocEdgeBuilder docEdgeBuilder = new DocEdgeBuilder(schema, edgeJson, graph);
            schema.add(docEdgeBuilder.build());
        });

        return schema;
    }
}
