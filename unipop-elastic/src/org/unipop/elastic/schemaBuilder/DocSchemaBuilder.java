package org.unipop.elastic.schemaBuilder;

import org.elasticsearch.client.Client;
import org.json.JSONObject;
import org.unipop.common.schema.builder.SchemaBuilder;
import org.unipop.elastic.common.ElasticHelper;
import org.unipop.elastic.document.schema.DocSchema;
import org.unipop.structure.UniGraph;

import java.io.IOException;

public abstract class DocSchemaBuilder<S extends DocSchema> extends SchemaBuilder<S> {
    protected String index;
    protected String type;

    public DocSchemaBuilder(JSONObject json, Client client, UniGraph graph) {
        super(json, graph);
        this.index = json.optString("index");
        this.type = json.optString("_type");

        try {
            ElasticHelper.createIndex(index, client);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public void setType(String type) {
        this.type = type;
    }
}
