package org.unipop.elastic2.controller.schema.helpers.schemaProviders;

import java.util.Optional;

/**
 * Created by Roman on 1/16/2015.
 */
public interface GraphElementSchemaProvider {
    public Optional<GraphVertexSchema> getVertexSchema(String type);
    public Optional<GraphEdgeSchema> getEdgeSchema(String type, Optional<String> sourceType, Optional<String> destinationType);
    public Optional<Iterable<GraphEdgeSchema>> getEdgeSchemas(String type);

    public Iterable<String> getVertexTypes();
    public Iterable<String> getEdgeTypes();
}
