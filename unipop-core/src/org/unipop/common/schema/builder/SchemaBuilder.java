package org.unipop.common.schema.builder;

import org.json.JSONObject;
import org.unipop.common.schema.property.PropertySchema;
import org.unipop.common.schema.ElementSchema;
import org.unipop.structure.UniGraph;

import java.util.List;

public abstract class SchemaBuilder<S extends ElementSchema> {
    protected final UniGraph graph;
    protected List<PropertySchema> propertySchemas;

    public SchemaBuilder(JSONObject json, UniGraph graph) {
        this.graph = graph;
        try {
            this.propertySchemas = PropertySchemaFactory.createPropertySchemas(json);
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    public abstract S build();
}
