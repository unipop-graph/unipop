package org.unipop.elastic2.controller.schema.helpers.queryAppenders;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.elastic2.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.structure.UniGraph;

import java.util.Optional;

/**
 * Created by Roman on 3/28/2015.
 */
public abstract class GraphQueryAppenderBase<TInput> implements QueryAppender<TInput> {
    //region Constructor
    public GraphQueryAppenderBase(
            UniGraph graph,
            GraphElementSchemaProvider schemaProvider,
            Optional<Direction> direction) {
        this.graph = graph;
        this.schemaProvider = schemaProvider;
        this.direction = direction;
    }
    //endregion

    //region Properties
    public UniGraph getGraph() {
        return this.graph;
    }

    public Optional<Direction> getDirection() {
        return this.direction;
    }

    public GraphElementSchemaProvider getSchemaProvider() {
        return this.schemaProvider;
    }
    //endregion

    //region Fields
    private UniGraph graph;
    private GraphElementSchemaProvider schemaProvider;
    private Optional<Direction> direction;
    //endregion
}
