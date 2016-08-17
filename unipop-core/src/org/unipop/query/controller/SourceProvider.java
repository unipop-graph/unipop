package org.unipop.query.controller;

import org.json.JSONObject;
import org.unipop.schema.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public interface SourceProvider {
    Set<UniQueryController> init(UniGraph graph, JSONObject configuration) throws Exception;
    default List<PropertySchema.PropertySchemaBuilder> providerBuilders() {return Collections.emptyList();}
    void close();
}
