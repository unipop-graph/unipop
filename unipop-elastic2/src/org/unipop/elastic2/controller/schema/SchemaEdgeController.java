package org.unipop.elastic2.controller.schema;

import com.google.common.collect.FluentIterable;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.search.SearchHit;
import org.javatuples.Pair;
import org.unipop.query.UniQuery;
import org.unipop.elastic2.controller.schema.helpers.ElasticGraphConfiguration;
import org.unipop.elastic2.controller.schema.helpers.QueryBuilder;
import org.unipop.elastic2.controller.schema.helpers.SearchAggregationIterable;
import org.unipop.elastic2.controller.schema.helpers.SearchBuilder;
import org.unipop.elastic2.controller.schema.helpers.aggregationConverters.CompositeAggregation;
import org.unipop.elastic2.controller.schema.helpers.elementConverters.ElementConverter;
import org.unipop.elastic2.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic2.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.elastic2.helpers.AggregationHelper;
import org.unipop.elastic2.helpers.ElasticMutations;
import org.unipop.elastic2.controller.schema.helpers.queryAppenders.*;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniVertex;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.function.BiFunction;

public class SchemaEdgeController extends SchemaElementController implements EdgeQueryController {
    //region ctor
    public SchemaEdgeController(UniGraph graph,
                                GraphElementSchemaProvider schemaProvider,
                                Client client,
                                ElasticMutations elasticMutations,
                                ElasticGraphConfiguration elasticGraphConfiguration,
                                ElementConverter<Element, Element> elementConverter
    ) {
        super(graph, schemaProvider, client, elasticMutations, elasticGraphConfiguration, elementConverter);
    }
    //endregion

    //region EdgeHandler Implementation
    @Override
    public Iterator<UniEdge> edges(UniQuery uniQuery) {
        return (Iterator<UniEdge>) elements(uniQuery, Edge.class);
    }

    @Override
    public Iterator<UniEdge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, UniQuery uniQuery) {
        SearchBuilder searchBuilder = buildEdgesQuery(edgeLabels, uniQuery);

        if (!appendQuery(Arrays.asList(vertices), getQueryAppender(direction), searchBuilder)) {
            return Collections.<UniEdge>emptyList().iterator();
        }

        Iterable<SearchHit> scrollIterable = getSearchHits(searchBuilder);
        return transformSearchHitsToElements(scrollIterable);
    }

    @Override
    public long edgeCount(UniQuery uniQuery) {
        SearchBuilder searchBuilder = buildElementsQuery(uniQuery, Edge.class);

        long count = 0;
        try {
            SearchResponse response = searchBuilder.getSearchRequest(client).setSearchType(SearchType.COUNT).execute().get();
            count += response.getHits().getTotalHits();
        } catch(Exception ex) {
            //TODO: decide what to do here
            return 0L;
        }

        return count;
    }

    @Override
    public long edgeCount(Vertex[] vertices, Direction direction, String[] edgeLabels, UniQuery uniQuery) {
        HashMap<String, Pair<Long, Vertex>> idsCount = AggregationHelper.getIdsCounts(Arrays.asList(vertices));

        SearchBuilder searchBuilder = buildEdgesQuery(edgeLabels, uniQuery);
        if (!appendQuery(Arrays.asList(vertices), getQueryAppender(direction), searchBuilder)) {
            return 0L;
        }
        appendAggregationsForMultipleVertices(searchBuilder);

        SearchAggregationIterable aggregations = new SearchAggregationIterable(
                this.graph,
                searchBuilder.getSearchRequest(this.client),
                this.client);
        CompositeAggregation compositeAggregation = new CompositeAggregation(null, aggregations);

        Map<String, Object> result = AggregationHelper.getAggregationConverter(searchBuilder.getAggregationBuilder(), false).convert(compositeAggregation);

        return AggregationHelper.countResultsWithRespectToOriginalOccurrences(idsCount, result);
    }

    @Override
    public Map<String, Object> edgeGroupBy(UniQuery uniQuery, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        SearchBuilder searchBuilder = buildElementsQuery(uniQuery, Edge.class);

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
    public Map<String, Object> edgeGroupBy(Vertex[] vertices, Direction direction, String[] edgeLabels, UniQuery uniQuery, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        SearchBuilder searchBuilder = buildEdgesQuery(edgeLabels, uniQuery);
        if (!appendQuery(Arrays.asList(vertices), getQueryAppender(direction), searchBuilder)) {
            return new HashMap<>();
        }

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
    public UniEdge addEdge(Object edgeId, String label, UniVertex outV, UniVertex inV, Map<String, Object> properties) {
        Optional<GraphEdgeSchema> edgeSchema = this.schemaProvider.getEdgeSchema(label, Optional.of(outV.label()), Optional.of(inV.label()));
        SchemaEdge elasticEdge = new SchemaEdge(edgeId, label, properties, outV, inV, this, graph, edgeSchema, this.elasticMutations);

        if (!edgeSchema.isPresent()) {
            // add to default??
            return null;
        }

        try {
            String writeIndex = FluentIterable.from(edgeSchema.get().getIndices()).first().get();
            elasticMutations.addElement(elasticEdge, writeIndex, null, true);
        }
        catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.edgeWithIdAlreadyExists(elasticEdge.id());
        }
        return elasticEdge;
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
    protected Iterator<UniEdge> transformSearchHitsToElements(Iterable<SearchHit> scrollIterable) {
        // SearchHit --> SearchHitElement --> Vertex
        return FluentIterable.from(scrollIterable).
                transformAndConcat(searchHit -> getElementConverter().convert(this.getSearchHitElementFactory().getElement(searchHit))).
                transform(element -> (UniEdge) element).iterator();
    }
    //endregion

    //region Protected Methods
    protected QueryAppender<GraphBulkInput> getQueryAppender(Direction direction) {
        return new CompositeQueryAppender<GraphBulkInput>(
                CompositeQueryAppender.Mode.All,
                new GraphBulkQueryAppender(
                        GraphBulkQueryAppender.TypeMode.cartesian,
                        new CompositeQueryAppender<>(
                                CompositeQueryAppender.Mode.First,
                                new DualEdgeQueryAppender(this.graph, this.schemaProvider, Optional.of(direction)),
                                new SingleEdgeQueryAppender(this.graph, this.schemaProvider, Optional.of(direction)))
                        ));
    }

    protected boolean appendQuery(Iterable<Vertex> vertices, QueryAppender<GraphBulkInput> queryAppender, SearchBuilder searchBuilder) {
        final Map<String, Iterable<String>> map = new HashMap<>();
        vertices.forEach(vertex -> {
            Set<String> typeElementIds = (Set<String>) map.get(vertex.label());
            if (typeElementIds == null) {
                typeElementIds = new HashSet<>();
                map.put(vertex.label(), typeElementIds);
            }

            typeElementIds.add(vertex.id().toString());
        });

        return queryAppender.append(new GraphBulkInput(
                map,
                searchBuilder.getTypes(),
                searchBuilder.getQueryBuilder()));
    }
    //endregion

    //region Private methods

    private void appendAggregationsForMultipleVertices(SearchBuilder searchBuilder) {
        BiFunction biFunction = new BiFunction<QueryBuilder.Composite, Set<String>, Set<String>>() {
            @Override
            public Set<String> apply(QueryBuilder.Composite composite, Set<String> strings) {
                if (composite.getOp() == QueryBuilder.Op.term || composite.getOp() == QueryBuilder.Op.terms) {
                    strings.add(composite.getName());
                }
                return strings;
            }
        };
        Set fields = new HashSet<>();
        searchBuilder.getQueryBuilder().<Set<String>>visit("appenders", biFunction, fields);
        for (Object field : fields){
            searchBuilder.getAggregationBuilder().seekRoot().terms(field.toString()).field(field.toString()).size(0);
        }
    }


    private SearchBuilder buildEdgesQuery(String[] edgeLabels, UniQuery uniQuery) {
        this.elasticMutations.refreshIfDirty();

        SearchBuilder searchBuilder = new SearchBuilder();
        searchBuilder.getIncludeSourceFields().add("*");
        searchBuilder.setLimit(configuration.getElasticGraphDefaultSearchSize());

        translateLabelsPredicate(Arrays.asList(edgeLabels), searchBuilder, Edge.class);
        translateHasContainers(searchBuilder, uniQuery.hasContainers);
//        translateLimits(uniQuery.limitHigh, searchBuilder);
        translateLimits(10000, searchBuilder);
        return searchBuilder;
    }
    //endregion
}
