package org.unipop.elastic2.controller.schema.helpers.schemaProviders;



import java.util.HashMap;
import java.util.Optional;

/**
 * Created by Roman on 3/21/2015.
 */
public class CachedGraphElementSchemaProvider implements GraphElementSchemaProvider {
    //region Constructor
    public CachedGraphElementSchemaProvider(GraphElementSchemaProvider schemaProvider) {
        this.schemaProvider = schemaProvider;

        this.vertexSchemaCache = new HashMap<>();
        this.edgeSchemaCache = new HashMap<>();
        this.edgeSchemasCache = new HashMap<>();
    }
    //endregion

    //region GraphElementSchemaProvider Implementation
    @Override
    public Optional<GraphVertexSchema> getVertexSchema(String type) {
        Optional<GraphVertexSchema> vertexSchema = vertexSchemaCache.get(type);
        if (vertexSchema == null) {
            vertexSchema = schemaProvider.getVertexSchema(type);
            vertexSchemaCache.put(type, vertexSchema);
        }

        return vertexSchema;
    }

    @Override
    public Optional<GraphEdgeSchema> getEdgeSchema(String type, Optional<String> sourceType, Optional<String> destinationType) {
        String schemaKey = String.format(
                "%s.%s.%s",
                sourceType != null ? sourceType.isPresent() ? sourceType.get() : "null" : "null",
                type,
                destinationType != null ? destinationType.isPresent() ? destinationType.get() : "null" : "null");

        Optional < GraphEdgeSchema> edgeSchema = edgeSchemaCache.get(schemaKey);
        if (edgeSchema == null) {
            edgeSchema = schemaProvider.getEdgeSchema(type, sourceType, destinationType);
            edgeSchemaCache.put(schemaKey, edgeSchema);
        }

        return edgeSchema;
    }

    @Override
    public Optional<Iterable<GraphEdgeSchema>> getEdgeSchemas(String type) {
        Optional<Iterable<GraphEdgeSchema>> edgeSchemas = edgeSchemasCache.get(type);
        if (edgeSchemas == null) {
            edgeSchemas = schemaProvider.getEdgeSchemas(type);
            edgeSchemasCache.put(type, edgeSchemas);
        }

        return edgeSchemas;
    }

    @Override
    public Iterable<String> getVertexTypes() {
        if (cachedVertexTypes == null) {
            cachedVertexTypes = schemaProvider.getVertexTypes();
        }

        return cachedVertexTypes;
    }

    @Override
    public Iterable<String> getEdgeTypes() {
        if (cachedEdgeTypes == null) {
            cachedEdgeTypes = schemaProvider.getEdgeTypes();
        }

        return cachedEdgeTypes;
    }
    //endregion

    //region Fields
    private GraphElementSchemaProvider schemaProvider;
    private HashMap<String, Optional<GraphVertexSchema>> vertexSchemaCache;
    private HashMap<String, Optional<GraphEdgeSchema>> edgeSchemaCache;
    private HashMap<String, Optional<Iterable<GraphEdgeSchema>>> edgeSchemasCache;

    private Iterable<String> cachedVertexTypes;
    private Iterable<String> cachedEdgeTypes;
    //endregion
}
