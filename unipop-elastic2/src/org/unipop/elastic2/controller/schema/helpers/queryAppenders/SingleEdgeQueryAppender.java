package org.unipop.elastic2.controller.schema.helpers.queryAppenders;


import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.elastic2.controller.schema.helpers.elementConverters.utils.EdgeHelper;
import org.unipop.elastic2.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic2.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.structure.UniGraph;

import java.util.Optional;

/**
 * Created by Roman on 3/28/2015.
 */
public class SingleEdgeQueryAppender extends GraphQueryAppenderBase<GraphTypeBulkInput> {
    //region Constructor
    public SingleEdgeQueryAppender(
            UniGraph graph,
            GraphElementSchemaProvider schemaProvider, Optional<Direction> direction) {
        super(graph, schemaProvider, direction);
    }
    //endregion

    //region QueryAppender Implementation
    @Override
    public boolean canAppend(GraphTypeBulkInput input) {
        Optional<GraphEdgeSchema> edgeSchema = this.getSchemaProvider().getEdgeSchema(input.getTypeToQuery(), Optional.of(input.getElementType()), null);

        if (!edgeSchema.isPresent()) {
            edgeSchema = this.getSchemaProvider().getEdgeSchema(input.getTypeToQuery(), null, Optional.of(input.getElementType()));
            if (!edgeSchema.isPresent()) {
                return false;
            }
        }

        if (!getDirection().isPresent()) {
            return false;
        }

        Optional<GraphEdgeSchema.Direction> edgeDirection = edgeSchema.get().getDirection();
        if (edgeDirection.isPresent()) {
            return false;
        }

        String sourceType = EdgeHelper.getEdgeSourceType(edgeSchema.get(), null);
        String destinationType = EdgeHelper.getEdgeDestinationType(edgeSchema.get(), null);

        return (sourceType != null && sourceType.equals(input.getElementType())) ||
                (destinationType != null && destinationType.equals(input.getElementType()));
    }

    @Override
    public boolean append(GraphTypeBulkInput input) {
        Optional<GraphEdgeSchema> edgeSchema = this.getSchemaProvider().getEdgeSchema(input.getTypeToQuery(), Optional.of(input.getElementType()), null);

        if (!edgeSchema.isPresent()) {
            edgeSchema = this.getSchemaProvider().getEdgeSchema(input.getTypeToQuery(), null, Optional.of(input.getElementType()));
            if (!edgeSchema.isPresent()) {
                return false;
            }
        }

        Optional<GraphEdgeSchema.Direction> edgeDirection = edgeSchema.get().getDirection();
        if (edgeDirection.isPresent()) {
            return false;
        }

        if (!getDirection().isPresent()) {
            return false;
        }

        String sourceType = EdgeHelper.getEdgeSourceType(edgeSchema.get(), null);
        String destinationType = EdgeHelper.getEdgeDestinationType(edgeSchema.get(), null);

        boolean appendedSuccesfully = false;

        switch (getDirection().get()) {
            case OUT:
                if (sourceType != null && sourceType.equals(input.getElementType())) {
                    input.getQueryBuilder().seekRoot().query().filtered()
                            .filter().bool().should("appenders")
                            .terms(edgeSchema.get().getSource().get().getIdField(), input.getElementIds());
                    appendedSuccesfully = true;
                }
                break;

            case IN:
                if (destinationType != null && destinationType.equals(input.getElementType())) {
                    input.getQueryBuilder().seekRoot().query().filtered()
                            .filter().bool().should("appenders")
                            .terms(edgeSchema.get().getDestination().get().getIdField(), input.getElementIds());
                    appendedSuccesfully = true;
                }
                break;

            default:
                if (edgeSchema.get().getSource().isPresent()) {
                    if (edgeSchema.get().getDestination().isPresent()) {
                        if (sourceType != null && sourceType.equals(input.getElementType())) {
                            input.getQueryBuilder().seekRoot().query().filtered()
                                    .filter().bool().should("appenders").bool().should()
                                    .terms(edgeSchema.get().getSource().get().getIdField(), input.getElementIds());
                            appendedSuccesfully = true;
                        }

                        if (destinationType != null && destinationType.equals(input.getElementType())) {
                            input.getQueryBuilder().seekRoot().query().filtered()
                                    .filter().bool().should("appenders").bool().should()
                                    .terms(edgeSchema.get().getDestination().get().getIdField(), input.getElementIds());
                            appendedSuccesfully = true;
                        }
                    }
                    else {
                        if (sourceType != null && sourceType.equals(input.getElementType())) {
                            input.getQueryBuilder().seekRoot().query().filtered()
                                    .filter().bool().should("appenders")
                                    .terms(edgeSchema.get().getSource().get().getIdField(), input.getElementIds());
                            appendedSuccesfully = true;
                        }
                    }
                } else if (edgeSchema.get().getDestination().isPresent()) {
                    if (destinationType != null && destinationType.equals(input.getElementType())) {
                        input.getQueryBuilder().seekRoot().query().filtered()
                                .filter().bool().should("appenders")
                                .terms(edgeSchema.get().getDestination().get().getIdField(), input.getElementIds());
                        appendedSuccesfully = true;
                    }
                }
        }

        return appendedSuccesfully;
    }
    //endregion
}
