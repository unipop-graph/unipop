package org.unipop.elastic.schemaBuilder;

import org.elasticsearch.client.Client;
import org.json.JSONObject;
import org.unipop.common.schema.reference.ReferenceVertexSchema;
import org.unipop.common.schema.reference.ReferenceVertexSchemaBuilder;
import org.unipop.elastic.document.schema.DocEdgeSchema;
import org.unipop.structure.UniGraph;


public class DocEdgeBuilder extends DocSchemaBuilder<DocEdgeSchema> {
    private ReferenceVertexSchema outVertexSchema;
    private ReferenceVertexSchema inVertexSchema;

    public DocEdgeBuilder(JSONObject json, Client client, UniGraph graph) {
        super(json, client, graph);
        try {
            JSONObject outVertex = json.getJSONObject("outVertex");
            outVertexSchema = new ReferenceVertexSchemaBuilder(outVertex, graph).build();

            JSONObject inVertex = json.getJSONObject("inVertex");
            inVertexSchema = new ReferenceVertexSchemaBuilder(inVertex, graph).build();
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public DocEdgeSchema build() {
        return new DocEdgeSchema(index, type, outVertexSchema, inVertexSchema, propertySchemas, graph);
    }
}