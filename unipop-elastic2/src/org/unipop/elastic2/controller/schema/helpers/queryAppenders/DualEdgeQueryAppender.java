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
public class DualEdgeQueryAppender extends GraphQueryAppenderBase<GraphTypeBulkInput> {
    //region Constructor
    public DualEdgeQueryAppender(
            UniGraph graph,
            GraphElementSchemaProvider schemaProvider,
            Optional<Direction> direction) {
        super(graph,schemaProvider, direction);
    }
    //endregion

    //region QueryAppender Implementation
    @Override
    public boolean canAppend(GraphTypeBulkInput input) {
        Optional<GraphEdgeSchema> edgeSchema = this.getSchemaProvider().getEdgeSchema(input.getTypeToQuery(), Optional.of(input.getElementType()), null);

        if (!edgeSchema.isPresent()) {
            return false;
        }

        Optional<GraphEdgeSchema.Direction> edgeDirection = edgeSchema.get().getDirection();
        if (!edgeDirection.isPresent()) {
            return false;
        }

        String sourceType = EdgeHelper.getEdgeSourceType(edgeSchema.get(), null);
        return sourceType != null && sourceType.equals(input.getElementType());
    }

    @Override
    public boolean append(GraphTypeBulkInput input) {
        Optional<GraphEdgeSchema> edgeSchema = this.getSchemaProvider().getEdgeSchema(input.getTypeToQuery(), Optional.of(input.getElementType()), null);

        if (!edgeSchema.isPresent()) {
            return false;
        }

        Optional<GraphEdgeSchema.Direction> edgeDirection = edgeSchema.get().getDirection();
        if (!edgeDirection.isPresent()) {
            return false;
        }

        String sourceType = EdgeHelper.getEdgeSourceType(edgeSchema.get(), null);
        boolean appendedSuccesfully = false;
        if (sourceType != null && sourceType.equals(input.getElementType())) {
            if (getDirection().isPresent() && getDirection().get() != Direction.BOTH) {

                input.getQueryBuilder().seekRoot().query().filtered().filter().bool().should("appenders").bool().must()
                        .terms(edgeSchema.get().getSource().get().getIdField(), input.getElementIds())
                        .term(edgeDirection.get().getField(), getDirection().get() == Direction.IN ?
                                edgeDirection.get().getInValue() :
                                edgeDirection.get().getOutValue());
                appendedSuccesfully = true;

            } else {
                input.getQueryBuilder().seekRoot().query().filtered().filter().bool().should("appenders")
                        .terms(edgeSchema.get().getSource().get().getIdField(), input.getElementIds());
                appendedSuccesfully = true;
            }
        }

        return appendedSuccesfully;
    }
    //endregion
}
