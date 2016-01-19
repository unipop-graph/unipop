package org.unipop.elastic2.controller.vertex;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.elastic2.controller.edge.ElasticEdge;
import org.unipop.elastic2.controller.schema.helpers.AggregationBuilder;
import org.unipop.elastic2.controller.schema.helpers.SearchAggregationIterable;
import org.unipop.elastic2.controller.schema.helpers.aggregationConverters.CompositeAggregation;
import org.unipop.elastic2.helpers.*;
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
        BoolQueryBuilder boolQuery = ElasticHelper.createQueryBuilder(predicates.hasContainers);
        boolQuery.must(QueryBuilders.missingQuery(ElasticEdge.InId));
        return new QueryIterator<>(boolQuery, scrollSize, predicates.limitHigh, client, this::createVertex, timing, getDefaultIndex());
//        return new QueryIterator<>(boolQuery, scrollSize, 10000, client, this::createVertex, timing, getDefaultIndex());
    }

    @Override
    public BaseVertex vertex(Direction direction, Object vertexId, String vertexLabel) {
        return createLazyVertex(vertexId, vertexLabel, getLazyGetter(direction));
    }

    @Override
    public long vertexCount(Predicates predicates) {
        elasticMutations.refresh(defaultIndex);
        BoolQueryBuilder boolQuery = ElasticHelper.createQueryBuilder(predicates.hasContainers);
        boolQuery.must(QueryBuilders.missingQuery(ElasticEdge.InId));

        try {
            SearchResponse response = client.prepareSearch().setIndices(defaultIndex)
                    .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolQuery))
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
        BoolQueryBuilder boolQuery = ElasticHelper.createQueryBuilder(predicates.hasContainers);
        boolQuery.must(QueryBuilders.missingQuery(ElasticEdge.InId));

        AggregationBuilder aggregationBuilder = new AggregationBuilder();
        AggregationHelper.applyAggregationBuilder(aggregationBuilder, keyTraversal, reducerTraversal, 0, 0, "global_ordinal_hash");

        SearchRequestBuilder searchRequest = client.prepareSearch().setIndices(defaultIndex)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolQuery))
                .setSearchType(SearchType.COUNT);
        for(org.elasticsearch.search.aggregations.AggregationBuilder innerAggregation : aggregationBuilder.getAggregations()) {
            searchRequest.addAggregation(innerAggregation);
        }

        SearchAggregationIterable aggregations = new SearchAggregationIterable(this.graph, searchRequest, this.client);
        CompositeAggregation compositeAggregation = new CompositeAggregation(null, aggregations);

        Map<String, Object> result = AggregationHelper.getAggregationConverter(aggregationBuilder, true).convert(compositeAggregation);
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


}
