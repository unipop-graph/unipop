package org.unipop.elastic2.controller.schema.helpers.elementConverters;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.elastic2.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.structure.UniGraph;


/**
 * Created by Roman on 3/16/2015.
 */
public abstract class GraphElementConverterBase<TElementSource extends Element, TElementDest extends Element>
        implements ElementConverter<TElementSource, TElementDest> {
    //region Constructor
    public GraphElementConverterBase(
            UniGraph graph,
            GraphElementSchemaProvider schemaProvider) {
        this.graph = graph;
        this.schemaProvider = schemaProvider;
    }
    //endregion

    //region Properties
    protected UniGraph getGraph() {
        return graph;
    }

    protected GraphElementSchemaProvider getSchemaProvider() {
        return this.schemaProvider;
    }
    //endregion

    //region Fields
    private UniGraph graph;
    private GraphElementSchemaProvider schemaProvider;
    //endregion
}
