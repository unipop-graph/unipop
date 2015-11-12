package org.unipop.elastic.controller.vertex;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.controller.aggregation.SemanticKeyTraversal;
import org.unipop.controller.aggregation.SemanticReducerTraversal;
import org.unipop.elastic.controller.edge.ElasticEdge;
import org.unipop.elastic.controller.schema.helpers.AggregationBuilder;
import org.unipop.elastic.controller.schema.helpers.SearchAggregationIterable;
import org.unipop.elastic.controller.schema.helpers.SearchBuilder;
import org.unipop.elastic.controller.schema.helpers.aggregationConverters.*;
import org.unipop.elastic.helpers.*;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ElasticVertexController implements VertexController {
    protected UniGraph graph;
    protected Client client;
    protected ElasticMutations elasticMutations;
    protected final int scrollSize;
    protected TimingAccessor timing;
    private String defaultIndex;
    private Map<Direction, LazyGetter> lazyGetters;

    public ElasticVertexController(UniGraph graph, Client client, ElasticMutations elasticMutations, String defaultIndex,
                                   int scrollSize, TimingAccessor timing) {
        this.graph = graph;
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.defaultIndex = defaultIndex;
        this.scrollSize = scrollSize;
        this.timing = timing;
        this.lazyGetters = new HashMap<>();
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Map<String, Object> properties) {
        BaseVertex v = createVertex(id, label, properties);
        try {
            elasticMutations.addElement(v, getIndex(properties), null, true);
        } catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.vertexWithIdAlreadyExists(id);
        }
        return v;
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates) {
        elasticMutations.refresh(defaultIndex);
        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        boolFilter.must(FilterBuilders.missingFilter(ElasticEdge.InId));
        return new QueryIterator<>(boolFilter, 0, scrollSize, predicates.limitHigh, client,
                this::createVertex, timing, getDefaultIndex());
    }

    @Override
    public BaseVertex vertex(Direction direction, Object vertexId, String vertexLabel) {
        return createLazyVertex(vertexId, vertexLabel, getLazyGetter(direction));
    }

    @Override
    public long vertexCount(Predicates predicates) {
        elasticMutations.refresh(defaultIndex);
        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        boolFilter.must(FilterBuilders.missingFilter(ElasticEdge.InId));

        try {
            SearchResponse response = client.prepareSearch().setIndices(defaultIndex)
                    .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolFilter))
                    .setSearchType(SearchType.COUNT).execute().get();
            return response.getHits().getTotalHits();
        } catch(Exception ex) {
            //TODO: decide what to do here
            return 0L;
        }
    }

    @Override
    public Map<String, Object> vertexGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        elasticMutations.refresh(defaultIndex);
        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        boolFilter.must(FilterBuilders.missingFilter(ElasticEdge.InId));

        AggregationBuilder aggregationBuilder = new AggregationBuilder();
        applyAggregationBuilder(aggregationBuilder, keyTraversal, reducerTraversal);

        SearchRequestBuilder searchRequest = client.prepareSearch().setIndices(defaultIndex)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolFilter))
                .setSearchType(SearchType.COUNT);
        for(org.elasticsearch.search.aggregations.AggregationBuilder innerAggregation : aggregationBuilder.getAggregations()) {
            searchRequest.addAggregation(innerAggregation);
        }

        SearchAggregationIterable aggregations = new SearchAggregationIterable(this.graph, searchRequest, this.client);
        CompositeAggregation compositeAggregation = new CompositeAggregation(null, aggregations);

        Map<String, Object> result = this.getAggregationConverter(aggregationBuilder, true).convert(compositeAggregation);
        return result;
    }

    private LazyGetter getLazyGetter(Direction direction) {
        LazyGetter lazyGetter = lazyGetters.get(direction);
        if (lazyGetter == null || !lazyGetter.canRegister()) {
            lazyGetter = new LazyGetter(client, timing);
            lazyGetters.put(direction,
                    lazyGetter);
        }
        return lazyGetter;
    }

    protected ElasticVertex createLazyVertex(Object id, String label,  LazyGetter lazyGetter) {
        return new ElasticVertex(id, label, null, this, graph, lazyGetter, elasticMutations, getDefaultIndex());
    }

    protected ElasticVertex createVertex(Object id, String label, Map<String, Object> keyValues) {
        return new ElasticVertex(id, label, keyValues, this, graph, null, elasticMutations, getIndex(keyValues));
    }

    protected String getDefaultIndex() {
        return this.defaultIndex;
    }

    protected String getIndex(Map<String, Object> properties) {
        return getDefaultIndex();
    }

    protected void applyAggregationBuilder(AggregationBuilder aggregationBuilder, Traversal keyTraversal, Traversal reducerTraversal) {
        if (SemanticKeyTraversal.class.isAssignableFrom(keyTraversal.getClass())) {
            SemanticKeyTraversal semanticKeyTraversal = (SemanticKeyTraversal) keyTraversal;
            aggregationBuilder.terms("key")
                    .field(semanticKeyTraversal.getKey())
                    .size(0)
                    .shardSize(0)
                    .executionHint("global_ordinals_hash");

            if (reducerTraversal != null && SemanticReducerTraversal.class.isAssignableFrom(reducerTraversal.getClass())) {
                SemanticReducerTraversal semanticReducerTraversalInstance = (SemanticReducerTraversal)reducerTraversal;
                String reduceAggregationName = "reduce";
                switch (semanticReducerTraversalInstance.getType()) {
                    case count:
                        aggregationBuilder.count(reduceAggregationName)
                                .field(semanticReducerTraversalInstance.getKey());
                        break;

                    case min:
                        aggregationBuilder.min(reduceAggregationName)
                                .field(semanticReducerTraversalInstance.getKey());
                        break;

                    case max:
                        aggregationBuilder.max(reduceAggregationName)
                                .field(semanticReducerTraversalInstance.getKey());
                        break;

                    case cardinality:
                        aggregationBuilder.cardinality(reduceAggregationName)
                                .field(semanticReducerTraversalInstance.getKey())
                                .precisionThreshold(1000L);
                        break;
                }
            }
        }
    }

    protected MapAggregationConverter getAggregationConverter(AggregationBuilder aggregationBuilder, boolean useSimpleFormat) {

        MapAggregationConverter mapAggregationConverter = new MapAggregationConverter();

        FilteredMapAggregationConverter filteredMapAggregationConverter = new FilteredMapAggregationConverter(
                aggregationBuilder,
                mapAggregationConverter);

        FilteredMapAggregationConverter filteredStatsAggregationConverter = new FilteredMapAggregationConverter(
                aggregationBuilder,
                new StatsAggregationConverter()
        );

        CompositeAggregationConverter compositeAggregationConverter = new CompositeAggregationConverter(
                filteredMapAggregationConverter,
                filteredStatsAggregationConverter,
                new SingleValueAggregationConverter()
        );

        mapAggregationConverter.setInnerConverter(compositeAggregationConverter);
        mapAggregationConverter.setUseSimpleFormat(useSimpleFormat);
        return mapAggregationConverter;
    }
}
