package org.unipop.elastic.schemaBuilder;

import org.elasticsearch.client.Client;
import org.json.JSONObject;
import org.unipop.common.schema.EdgeSchema;
import org.unipop.elastic.document.schema.DocVertexSchema;
import org.unipop.structure.UniGraph;

import java.util.HashSet;
import java.util.Set;

import static org.unipop.common.util.ConversionUtils.getList;

public class DocVertexBuilder extends DocSchemaBuilder<DocVertexSchema> {

    protected Set<EdgeSchema> edgeSchemas = new HashSet<>();

    public DocVertexBuilder(JSONObject json, Client client, UniGraph graph) {
        super(json, client, graph);
        getList(json, "edges").forEach(edgeJson -> {
            DocEdgeBuilder docEdgeBuilder = new DocEdgeBuilder(edgeJson, client, graph);
            docEdgeBuilder.setIndex(this.index);
            docEdgeBuilder.setType(this.type);
            edgeSchemas.add(docEdgeBuilder.build());
        });
    }

    @Override
    public DocVertexSchema build() {
        return new DocVertexSchema(index, type, propertySchemas, edgeSchemas, graph);
    }

}
