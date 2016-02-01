package org.unipop.elastic2.controller.schema;

import com.google.common.collect.FluentIterable;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.search.SearchHit;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.elastic2.controller.schema.helpers.ElasticGraphConfiguration;
import org.unipop.elastic2.controller.schema.helpers.LazyGetterFactory;
import org.unipop.elastic2.controller.schema.helpers.SearchAggregationIterable;
import org.unipop.elastic2.controller.schema.helpers.SearchBuilder;
import org.unipop.elastic2.controller.schema.helpers.aggregationConverters.CompositeAggregation;
import org.unipop.elastic2.controller.schema.helpers.elementConverters.ElementConverter;
import org.unipop.elastic2.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.elastic2.controller.schema.helpers.schemaProviders.GraphVertexSchema;
import org.unipop.elastic2.helpers.AggregationHelper;
import org.unipop.elastic2.helpers.ElasticMutations;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

public class SchemaVertexController extends SchemaElementController implements VertexController {
    //region ctor
    public SchemaVertexController(UniGraph graph,
                                  GraphElementSchemaProvider schemaProvider,
                                  Client client,
                                  ElasticMutations elasticMutations,
                                  ElasticGraphConfiguration elasticGraphConfiguration,
                                  ElementConverter<Element, Element> elementConverter
    ) {
        super(graph, schemaProvider, client, elasticMutations, elasticGraphConfiguration, elementConverter);
        this.lazyGetterFactory = new LazyGetterFactory(client, schemaProvider);
    }

    //endregion

    //region VertexHandler Implementation
    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates) {
        return (Iterator<BaseVertex>) elements(predicates, Vertex.class);
    }

    @Override
    public BaseVertex vertex(Direction direction, Object vertexId, String vertexLabel) {
        return new SchemaVertex(vertexId, vertexLabel, graph, null, this, lazyGetterFactory.getLazyGetter(vertexLabel), schemaProvider, this.elasticMutations);
    }

    @Override
    public long vertexCount(Predicates predicates) {
        SearchBuilder searchBuilder = buildElementsQuery(predicates, Vertex.class);

        try {
            SearchResponse response = searchBuilder.getSearchRequest(client).setSearchType(SearchType.COUNT).execute().get();
            return response.getHits().getTotalHits();
        } catch(Exception ex) {
            //TODO: decide what to do here
            return 0L;
        }
    }

    @Override
    public Map<String, Object> vertexGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        SearchBuilder searchBuilder = buildElementsQuery(predicates, Vertex.class);

        this.applyAggregationBuilder(searchBuilder.getAggregationBuilder(), keyTraversal, reducerTraversal);

        SearchAggregationIterable aggregations = new SearchAggregationIterable(
                this.graph,
                searchBuilder.getSearchRequest(this.client),
                this.client);
        CompositeAggregation compositeAggregation = new CompositeAggregation(null, aggregations);

        Map<String, Object> result = AggregationHelper.getAggregationConverter(searchBuilder.getAggregationBuilder(), true).convert(compositeAggregation);
        return result;
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Map<String, Object> properties) {
        BaseVertex v = new SchemaVertex(id, label, graph, properties, this, lazyGetterFactory.getLazyGetter(label), this.schemaProvider, this.elasticMutations);

        Optional<GraphVertexSchema> vertexSchema = this.schemaProvider.getVertexSchema(label);
        if (!vertexSchema.isPresent()) {
            // add to default??throw new NotImplementedException();
            return null;
        }

        try {
            String writeIndex = FluentIterable.from(vertexSchema.get().getIndices()).first().get();
            elasticMutations.addElement(v, writeIndex, null, true);
        } catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.vertexWithIdAlreadyExists(id);
        }
        return v;
    }

    @Override
    public void init(Map<String, Object> conf, UniGraph graph) throws Exception {
        throw new NotImplementedException();
    }

    @Override
    public void close() {
        throw new NotImplementedException();
    }
    //endregion

    //region help methods
    @Override
    protected Iterator<Vertex> transformSearchHitsToElements(Iterable<SearchHit> scrollIterable) {
        // SearchHit --> SearchHitElement --> Vertex
        return FluentIterable.from(scrollIterable).
                transformAndConcat(searchHit -> getElementConverter().convert(this.getSearchHitElementFactory().getElement(searchHit))).
                transform(element -> (Vertex) element).iterator();
    }
    //endregion



    //region fields
    protected LazyGetterFactory lazyGetterFactory;
    //endregion
}
