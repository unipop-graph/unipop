package org.unipop.elastic.document.schema.builder;

import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.elastic.common.ElasticClient;
import org.unipop.schema.builder.SchemaBuilder;
import org.unipop.elastic.document.schema.DocSchema;
import org.unipop.structure.UniGraph;


public abstract class DocBuilder<S extends DocSchema> extends SchemaBuilder<S> {
    protected String index;
    protected String type;

    public DocBuilder(JSONObject json, ElasticClient client, UniGraph graph) throws JSONException {
        super(json, graph);
        this.index = json.getString("index");
        this.type = json.optString("type", null);

        client.validateIndex(index);
    }
}
