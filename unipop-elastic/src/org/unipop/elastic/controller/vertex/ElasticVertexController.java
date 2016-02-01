package org.unipop.elastic.controller.vertex;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.elastic.controller.edge.ElasticEdge;
import org.unipop.elastic.controller.schema.helpers.AggregationBuilder;
import org.unipop.elastic.controller.schema.helpers.SearchAggregationIterable;
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
    protected int scrollSize;
    protected TimingAccessor timing;
    private String defaultIndex;
    private Map<Direction, ElasticLazyGetter> lazyGetters;

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

    public ElasticVertexController(){}

    @Override
    public void init(Map<String, Object> conf, UniGraph graph) throws Exception {
        this.lazyGetters = new HashMap<>();
        this.graph = graph;
        this.client = ((Client) conf.get("client"));
        this.elasticMutations = ((ElasticMutations) conf.get("elasticMutations"));
        this.timing = ((TimingAccessor) conf.get("timing"));
        this.defaultIndex = conf.getOrDefault("defaultIndex", "unipop_es1").toString();
        this.scrollSize = Integer.parseInt(conf.getOrDefault("scrollSize", "0").toString());

    }

    @Override
    public void close() {
        client.close();
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
        QueryBuilder query = ElasticHelper.createQuery(predicates.hasContainers, FilterBuilders.missingFilter(ElasticEdge.InId));
            return new QueryIterator<>(query, scrollSize, predicates.limitHigh, client,
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
        } catch (Exception ex) {
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
        AggregationHelper.applyAggregationBuilder(aggregationBuilder, keyTraversal, reducerTraversal, 0, 0, "global_ordinal_hash");

        SearchRequestBuilder searchRequest = client.prepareSearch().setIndices(defaultIndex)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolFilter))
                .setSearchType(SearchType.COUNT);
        for (org.elasticsearch.search.aggregations.AggregationBuilder innerAggregation : aggregationBuilder.getAggregations()) {
            searchRequest.addAggregation(innerAggregation);
        }

        SearchAggregationIterable aggregations = new SearchAggregationIterable(this.graph, searchRequest, this.client);
        CompositeAggregation compositeAggregation = new CompositeAggregation(null, aggregations);

        Map<String, Object> result = AggregationHelper.getAggregationConverter(aggregationBuilder, true).convert(compositeAggregation);
        return result;
    }

    private ElasticLazyGetter getLazyGetter(Direction direction) {
        ElasticLazyGetter elasticLazyGetter = lazyGetters.get(direction);
        if (elasticLazyGetter == null || !elasticLazyGetter.canRegister()) {
            elasticLazyGetter = new ElasticLazyGetter(client, timing);
            lazyGetters.put(direction,
                    elasticLazyGetter);
        }
        return elasticLazyGetter;
    }

    protected ElasticVertex createLazyVertex(Object id, String label, ElasticLazyGetter elasticLazyGetter) {
        return new ElasticVertex(id, label, null, this, graph, elasticLazyGetter, elasticMutations, getDefaultIndex());
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
