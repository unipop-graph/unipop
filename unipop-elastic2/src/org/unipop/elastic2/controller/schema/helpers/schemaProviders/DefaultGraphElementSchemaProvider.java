package org.unipop.elastic2.controller.schema.helpers.schemaProviders;

import java.util.Collections;
import java.util.Optional;

/**
 * Created by Roman on 3/19/2015.
 */
public class DefaultGraphElementSchemaProvider implements GraphElementSchemaProvider {
    //region Constructor
    public DefaultGraphElementSchemaProvider(Iterable<String> indices) {
        this.indices = indices;
    }
    //endregion

    //region GraphElementSchemaProvider Implementation
    @Override
    public Optional<GraphVertexSchema> getVertexSchema(String type) {
        return Optional.of(new GraphVertexSchema() {
            @Override
            public String getType() {
                return type;
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return indices;
            }
        });
    }

    @Override
    public Optional<GraphEdgeSchema> getEdgeSchema(String type, Optional<String> sourceType, Optional<String> destinationType) {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<GraphEdgeSchema.End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "sId";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.empty();
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "dId";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.empty();
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return type;
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return indices;
            }
        });
    }

    @Override
    public Optional<Iterable<GraphEdgeSchema>> getEdgeSchemas(String type) {
        return Optional.empty();
    }

        @Override
    public Iterable<String> getVertexTypes() {
        return Collections.emptyList();
    }

    @Override
    public Iterable<String> getEdgeTypes() {
        return Collections.emptyList();
    }
    //endregion

    //region Fields
    private Iterable<String> indices;
    //endregion
}
