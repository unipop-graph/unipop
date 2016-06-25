package org.unipop.elastic.document.schema.builder;

import org.json.JSONObject;
import org.unipop.schema.builder.SchemaBuilder;
import org.unipop.elastic.document.schema.DocSchema;
import org.unipop.structure.UniGraph;


public abstract class DocSchemaBuilder<S extends DocSchema> extends SchemaBuilder<S> {
    protected String index;
    protected String type;

    public DocSchemaBuilder(JSONObject json, UniGraph graph) {
        super(json, graph);
        this.index = json.optString("index", "*");
        this.type = json.optString("es_type", "*");
    }

    public DocSchemaBuilder(DocSchema parent, JSONObject json, UniGraph graph) {
        this(json, graph);
        this.index = parent.getIndex();
        this.type = parent.getType();
    }
}
