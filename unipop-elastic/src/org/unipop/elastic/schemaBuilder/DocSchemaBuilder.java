package org.unipop.elastic.schemaBuilder;

import org.elasticsearch.client.Client;
import org.json.JSONObject;
import org.unipop.schema.builder.SchemaBuilder;
import org.unipop.elastic.document.schema.DocSchema;
import org.unipop.structure.UniGraph;


public abstract class DocSchemaBuilder<S extends DocSchema> extends SchemaBuilder<S> {
    protected final Client client;
    protected String index;
    protected String type;

    public DocSchemaBuilder(JSONObject json, Client client, UniGraph graph) {
        super(json, graph);
        this.client = client;
        this.index = json.optString("index");
        this.type = json.optString("_type");
    }

    public DocSchemaBuilder(DocSchema parent, JSONObject json, Client client, UniGraph graph) {
        this(json, client, graph);
        this.index = parent.getIndex();
        this.type = parent.getType();
    }
}
