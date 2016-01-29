package org.unipop.elastic2.controller.schema.helpers.elementConverters;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.elastic2.controller.schema.helpers.LazyGetterFactory;
import org.unipop.elastic2.controller.schema.helpers.elementConverters.utils.VertexHelper;
import org.unipop.elastic2.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.elastic2.controller.schema.helpers.schemaProviders.GraphVertexSchema;
import org.unipop.elastic2.helpers.ElasticMutations;
import org.unipop.structure.UniGraph;

import java.util.Arrays;
import java.util.Optional;

/**
 * Created by Roman on 3/24/2015.
 */
public class VertexConverter extends GraphElementConverterBase<Element, Element> {
    //region Constructor
    public VertexConverter(
            UniGraph graph,
            GraphElementSchemaProvider schemaProvider,
            ElasticMutations elasticMutations,
            LazyGetterFactory lazyGetterFactory
            ) {
        super(graph, schemaProvider);
        this.elasticMutations = elasticMutations;
        this.lazyGetterFactory = lazyGetterFactory;
    }
    //endregion

    //region GraphElementConverterBase SearchHit Implementation
    @Override
    public boolean canConvert(Element element) {
        Optional<GraphVertexSchema> vertexSchema = this.getSchemaProvider().getVertexSchema(element.label());
        return vertexSchema.isPresent();
    }

    @Override
    public Iterable<Element> convert(Element element) {
        Vertex vertex = VertexHelper.createVertex(element, this.getSchemaProvider(), this.elasticMutations, this.lazyGetterFactory.getLazyGetter(element.label()));
        return Arrays.asList(vertex);
    }
    //endregion

    //region Fields
    protected ElasticMutations elasticMutations;
    private final LazyGetterFactory lazyGetterFactory;
    //endregion
}
