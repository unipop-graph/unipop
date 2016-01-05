package org.unipop.elastic2.controller.schema.helpers.elementConverters;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.elastic2.controller.schema.SchemaVertex;
import org.unipop.elastic2.controller.schema.helpers.LazyGetterFactory;
import org.unipop.elastic2.controller.schema.helpers.elementConverters.utils.EdgeHelper;
import org.unipop.elastic2.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic2.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.elastic2.helpers.ElasticMutations;
import org.unipop.structure.UniGraph;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

/**
 * Created by Roman on 3/24/2015.
 */
public class DualEdgeConverter extends GraphElementConverterBase<Element, Element> {
    //region Constructor
    public DualEdgeConverter(
            UniGraph graph,
            GraphElementSchemaProvider schemaProvider,
            ElasticMutations elasticMutations,
            LazyGetterFactory lazyGetterFactory) {
        super(graph, schemaProvider);
        this.elasticMutations = elasticMutations;
        this.lazyGetterFactory = lazyGetterFactory;
    }
    //endregion

    //region SearchHitElementConverterBase Implementation
    public boolean canConvert(Element edge) {
        Optional<GraphEdgeSchema> edgeSchema =  EdgeHelper.getEdgeSchema(this.getSchemaProvider(), edge);
        if (!edgeSchema.isPresent()) {
            return false;
        }

        if (!edgeSchema.get().getDirection().isPresent()) {
            return false;
        }

        return true;
    }

    // Assumption: The search query matches the current converter, meaning that measures were taken to make sure that the converter is appropriate for the search hits. If those measures were not taken,
    // converter could produce, and most likely will, wrong results. A more safe converter could be created in the expense of performance, but if done right the impact could be insignificant.
    @Override
    public Iterable<Element> convert(Element edge) {
        Optional<GraphEdgeSchema> edgeSchema =  EdgeHelper.getEdgeSchema(this.getSchemaProvider(), edge);
        Optional<Direction> direction = EdgeHelper.getEdgeDirection(edge, edgeSchema.get());

        if (!direction.isPresent()) {
            //TODO: what to do
        }

        SchemaVertex outVertex = new SchemaVertex(
                EdgeHelper.getEdgeSourceId(edge, edgeSchema.get(), null),
                EdgeHelper.getEdgeSourceType(edgeSchema.get(), null),
                (UniGraph)edge.graph(),
                null,
                null,
                lazyGetterFactory.getLazyGetter(EdgeHelper.getEdgeSourceType(edgeSchema.get(), null)),
                this.getSchemaProvider(),
                this.elasticMutations);

        SchemaVertex inVertex = new SchemaVertex(
                EdgeHelper.getEdgeDestinationId(edge, edgeSchema.get(), null),
                EdgeHelper.getEdgeDestinationType(edgeSchema.get(), null),
                (UniGraph)edge.graph(),
                null,
                null,
                lazyGetterFactory.getLazyGetter(EdgeHelper.getEdgeSourceType(edgeSchema.get(), null)),
                this.getSchemaProvider(),
                this.elasticMutations);

        switch (direction.get()) {
            case OUT:
                return Arrays.asList(EdgeHelper.createEdge(edge, inVertex, outVertex, this.getSchemaProvider(), elasticMutations));

            case IN:
                return Arrays.asList(EdgeHelper.createEdge(edge, outVertex, inVertex, this.getSchemaProvider(), elasticMutations));

            default: return Collections.emptyList();
        }
    }
    //endregion

    //region Fields
    protected ElasticMutations elasticMutations;
    protected LazyGetterFactory lazyGetterFactory;
    //endregion
}
