package org.unipop.elastic.document.schema.builder;

import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.elastic.common.ElasticClient;
import org.unipop.schema.VertexSchema;
import org.unipop.schema.reference.ReferenceVertexSchemaBuilder;
import org.unipop.elastic.document.schema.DocEdgeSchema;
import org.unipop.structure.UniGraph;


public class DocEdgeBuilder extends DocBuilder<DocEdgeSchema> {

    private VertexSchema outVertexSchema;
    private VertexSchema inVertexSchema;

    public DocEdgeBuilder(JSONObject json, ElasticClient client, UniGraph graph) throws JSONException {
        super(json, client, graph);
        this.outVertexSchema = buildVertexSchema("outVertex");
        this.inVertexSchema = buildVertexSchema("inVertex");
    }

    private VertexSchema buildVertexSchema(String key) {
        try {
            JSONObject json = this.json.getJSONObject(key);
            return createVertexSchema(json);
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    protected VertexSchema createVertexSchema(JSONObject json) throws JSONException {
        if(json.optBoolean("ref", false)) return new ReferenceVertexSchemaBuilder(json, graph).build();
        return new InnerVertexBuilder(index, type, json, graph).build();
    }

    @Override
    public DocEdgeSchema build() {
        return new DocEdgeSchema(index, type, outVertexSchema, inVertexSchema, propertySchemas, graph);
    }
}