package org.unipop.elastic.document;

import io.searchbox.action.BulkableAction;
import io.searchbox.core.*;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unipop.elastic.common.ElasticClient;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.query.controller.SimpleController;
import org.unipop.query.mutation.AddEdgeQuery;
import org.unipop.query.mutation.AddVertexQuery;
import org.unipop.query.mutation.PropertyQuery;
import org.unipop.query.mutation.RemoveQuery;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.element.EdgeSchema;
import org.unipop.schema.element.ElementSchema;
import org.unipop.schema.element.VertexSchema;
import org.unipop.schema.reference.DeferredVertex;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;
import org.unipop.util.ConversionUtils;
import org.unipop.util.MetricsRunner;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class DocumentController implements SimpleController, LocalQuery.LocalController {
    private static final Logger logger = LoggerFactory.getLogger(DocumentController.class);

    private final ElasticClient client;
    private final UniGraph graph;

    private Set<? extends DocumentVertexSchema> vertexSchemas = new HashSet<>();
    private Set<? extends DocumentEdgeSchema> edgeSchemas = new HashSet<>();

    public DocumentController(Set<DocumentSchema> schemas, ElasticClient client, UniGraph graph) {
        this.client = client;
        this.graph = graph;

        Set<DocumentSchema> documentSchemas = collectSchemas(schemas);
        this.vertexSchemas = documentSchemas.stream().filter(schema -> schema instanceof DocumentVertexSchema)
                .map(schema -> ((DocumentVertexSchema) schema)).collect(Collectors.toSet());
        this.edgeSchemas = documentSchemas.stream().filter(schema -> schema instanceof DocumentEdgeSchema)
                .map(schema -> ((DocumentEdgeSchema) schema)).collect(Collectors.toSet());

        logger.debug("Instantiated DocumentController: {}", this);
    }

    @Override
    public Set<? extends VertexSchema> getVertexSchemas() {
        return vertexSchemas;
    }

    @Override
    public Set<? extends EdgeSchema> getEdgeSchemas() {
        return edgeSchemas;
    }

    private Set<DocumentSchema> collectSchemas(Set<? extends ElementSchema> schemas) {
        Set<DocumentSchema> docSchemas = new HashSet<>();

        schemas.forEach(schema -> {
            if (schema instanceof DocumentSchema) {
                docSchemas.add((DocumentSchema) schema);
                Set<DocumentSchema> childSchemas = collectSchemas(schema.getChildSchemas());
                docSchemas.addAll(childSchemas);
            }
        });
        return docSchemas;
    }

    @Override
    public String toString() {
        return "DocumentController{" +
                "client=" + client +
                ", vertexSchemas=" + vertexSchemas +
                ", edgeSchemas=" + edgeSchemas +
                '}';
    }

    //region Query Controller


    @Override
    public <E extends Element> Iterator<E> search(SearchQuery<E> uniQuery) {
        Set<? extends DocumentSchema<E>> schemas = getSchemas(uniQuery.getReturnType())
                .stream()
                .map(schema -> ((DocumentSchema<E>) schema))
                .collect(Collectors.toSet());
        SearchCollector<DocumentSchema<E>, QueryBuilder, E> collector = new SearchCollector<>((schema) -> schema.getSearch(uniQuery), (schema, results) -> schema.parseResults(results, uniQuery));
        Map<DocumentSchema<E>, QueryBuilder> searches = schemas.stream()
                .collect(collector);
        return search(uniQuery, searches, collector);
    }

    @Override
    public Iterator<Edge> search(SearchVertexQuery uniQuery) {
        SearchCollector<DocumentEdgeSchema, QueryBuilder, Edge> collector = new SearchCollector<>((schema) -> schema.getSearch(uniQuery), (schema, results) -> schema.parseResults(results, uniQuery));
        Map<DocumentEdgeSchema, QueryBuilder> schemas = edgeSchemas.stream()
                .collect(collector);
        return search(uniQuery, schemas, collector);
    }

    @Override
    public void fetchProperties(DeferredVertexQuery uniQuery) {
        SearchCollector<DocumentVertexSchema, QueryBuilder, Vertex> collector = new SearchCollector<>((schema) -> schema.getSearch(uniQuery), (schema, results) -> schema.parseResults(results, uniQuery));
        Map<DocumentVertexSchema, QueryBuilder> schemas = vertexSchemas.stream()
                .collect(collector);
        Iterator<Vertex> search = search(uniQuery, schemas, collector);

        Map<Object, DeferredVertex> vertexMap = uniQuery.getVertices().stream()
                .collect(Collectors.toMap(UniElement::id, Function.identity(), (a, b) -> a));
        search.forEachRemaining(newVertex -> {
            DeferredVertex deferredVertex = vertexMap.get(newVertex.id());
            if (deferredVertex != null) deferredVertex.loadProperties(newVertex);
        });
    }

    @Override
    public <S extends Element> Iterator<Pair<String, S>> local(LocalQuery<S> query) {
        SearchCollector<DocumentEdgeSchema, QueryBuilder, Pair<String, Element>> collector =
                new SearchCollector<>((schema) -> schema.getSearch((SearchVertexQuery) query.getSearchQuery()),
                        (schema, results) -> schema.parseLocal(results, query));
        Set<? extends DocumentEdgeSchema> schemas = edgeSchemas;
        Map<DocumentEdgeSchema, QueryBuilder> searches = schemas.stream()
                .collect(collector);
        Iterator<Pair<String, Element>> search = search((SearchQuery<Edge>) query.getSearchQuery(), searches, collector, (schema) -> schema.getLocal(query));
        return ConversionUtils.asStream(search).map(o -> ((Pair<String, S>) o)).iterator();
    }

    @Override
    public Edge addEdge(AddEdgeQuery uniQuery) {
        UniEdge edge = new UniEdge(uniQuery.getProperties(), uniQuery.getOutVertex(), uniQuery.getInVertex(), graph);
        try {
            if (index(this.edgeSchemas, edge, true)) return edge;
        } catch (DocumentAlreadyExistsException ex) {
            logger.warn("Document already exists in elastic", ex);
            throw Graph.Exceptions.edgeWithIdAlreadyExists(edge.id());
        }
        return null;
    }

    @Override
    public Vertex addVertex(AddVertexQuery uniQuery) {
        UniVertex vertex = new UniVertex(uniQuery.getProperties(), graph);
        try {
            if (index(this.vertexSchemas, vertex, true)) return vertex;
        } catch (DocumentAlreadyExistsException ex) {
            logger.warn("Document already exists in elastic", ex);
            throw Graph.Exceptions.vertexWithIdAlreadyExists(vertex.id());
        }
        return vertex;
    }

    @Override
    public <E extends Element> void property(PropertyQuery<E> uniQuery) {
        Set<? extends DocumentSchema<E>> schemas = getSchemas(uniQuery.getElement().getClass());
        //updates
        index(schemas, uniQuery.getElement(), false);
    }

    @Override
    public <E extends Element> void remove(RemoveQuery<E> uniQuery) {
        uniQuery.getElements().forEach(element -> {
            Set<? extends DocumentSchema<Element>> schemas = getSchemas(element.getClass());
            delete(schemas, element);
        });
    }

    private <E extends Element> Set<? extends DocumentSchema<E>> getSchemas(Class elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass))
            return (Set<? extends DocumentSchema<E>>) vertexSchemas;
        else return (Set<? extends DocumentSchema<E>>) edgeSchemas;
    }

    //endregion

    //region Elastic Queries

    private void fillChildren(List<MutableMetrics> childMetrics, SearchResult result) {
        if (childMetrics.size() > 0) {
            MutableMetrics child = childMetrics.get(0);
            child.setCount(TraversalMetrics.ELEMENT_COUNT_ID, result.getTotal());
            child.setDuration(Long.parseLong(result.getJsonObject().get("took").toString()), TimeUnit.MILLISECONDS);
        }
    }

    private <E extends Element, S extends DocumentSchema<E>> Pair<S, SearchSourceBuilder> createSearchBuilder(Map.Entry<S, QueryBuilder> kv, SearchQuery<E> query) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(kv.getValue())
                .size(query.getLimit() == -1 ? 10000 : query.getLimit());
        if (query.getPropertyKeys() == null) searchSourceBuilder.fetchSource(true);
        else {
            Set<String> fields = kv.getKey().toFields(query.getPropertyKeys());

            if (fields.size() == 0) searchSourceBuilder.fetchSource(false);
            else searchSourceBuilder.fetchSource(fields.toArray(new String[fields.size()]), null);
        }
        List<Pair<String, Order>> orders = query.getOrders();
        if (orders != null) {
            orders.forEach(order -> {
                Order orderValue = order.getValue1();
                String field = kv.getKey().getFieldByPropertyKey(order.getValue0());
                if (field != null) {
                    switch (orderValue) {
                        case decr:
                            searchSourceBuilder.sort(kv.getKey()
                                    .getFieldByPropertyKey(order.getValue0()), SortOrder.DESC);
                            break;
                        case incr:
                            searchSourceBuilder.sort(kv.getKey()
                                    .getFieldByPropertyKey(order.getValue0()), SortOrder.ASC);
                            break;
                        case shuffle:
                            break;
                    }
                }
            });
        }
        return Pair.with(kv.getKey(), searchSourceBuilder);
    }


    private <E extends Element, S extends DocumentSchema<E>> Pair<S, Search> createSearch(Pair<S, SearchSourceBuilder> kv, SearchQuery<E> query) {
        Search.Builder builder = new Search.Builder(kv.getValue1().toString().replace("\n", ""))
                .ignoreUnavailable(true).allowNoIndices(true);
        kv.getValue0().getIndex().getIndex(query.getPredicates()).forEach(builder::addIndex);
        builder.addType(kv.getValue0().getType());
        return Pair.with(kv.getValue0(), builder.build());
    }

    private <E extends Element, S extends DocumentSchema<E>, R> Iterator<R> search(SearchQuery<E> query, Map<S, QueryBuilder> schemas, SearchCollector<S, QueryBuilder, R> collector) {
        return search(query, schemas, collector, null);
    }

    private <E extends Element, S extends DocumentSchema<E>, R> Iterator<R> search(SearchQuery<E> query, Map<S, QueryBuilder> schemas, SearchCollector<S, QueryBuilder, R> collector, Function<S, List<AggregationBuilder>> aggs) {
        if (schemas.size() == 0) return EmptyIterator.instance();
        logger.debug("Preparing search. Schemas: {}", schemas);

        client.refresh();

        return schemas.entrySet().parallelStream().filter(Objects::nonNull)
                .map(kv -> createSearchBuilder(kv, query))
                .map(kv -> {
                    if (aggs != null) {
                        List<AggregationBuilder> aggregationBuilders = aggs.apply(kv.getValue0());
                        SearchSourceBuilder searchSourceBuilder = kv.getValue1();
                        aggregationBuilders.forEach(searchSourceBuilder::aggregation);
                    }
                    return kv;
                })
                .map(kv -> createSearch(kv, query))
                .map(kv -> {
                    MetricsRunner metrics = new MetricsRunner(this, query,
                            Collections.singletonList(kv.getValue0()));
                    SearchResult results = client.execute(kv.getValue1());
                    metrics.stop((children -> fillChildren(children, results)));
                    if (results == null || !results.isSucceeded()) return new ArrayList<E>();
                    return collector.parse.apply(kv.getValue0(), results.getJsonString());
                }).flatMap(Collection::stream).map(e -> (R) e).iterator();

    }

    private boolean valid(MultiSearchResult.MultiSearchResponse multiSearchResponse) {
        if (multiSearchResponse.isError) {
            logger.error("failed to execute multiSearch: {}", multiSearchResponse);
            return false;
        }
        return true;
    }

    private <E extends Element> boolean index(Set<? extends DocumentSchema<E>> schemas, E element, boolean create) {
        for (DocumentSchema<E> schema : schemas) {
            BulkableAction<DocumentResult> action = schema.addElement(element, create);
            if (action != null) {
                logger.debug("indexing element with schema: {}, element: {}, index: {}, client: {}", schema, element, action, client);
                client.bulk(element, action);
                return true;
            }
        }
        return false
                ;
    }

    private <E extends Element> void delete(Set<? extends DocumentSchema<E>> schemas, E element) {
        for (DocumentSchema<E> schema : schemas) {
            Delete.Builder delete = schema.delete(element);
            if (delete != null) {
                logger.debug("deleting element with schema: {}, element: {}, client: {}", schema, element, client);
                client.bulk(element, delete.build());
            }
        }
    }

    //endregion

    public class SearchCollector<K, V, R> implements Collector<K, Map<K, V>, Map<K, V>> {

        private final Function<? super K, ? extends V> valueMapper;

        private final BiFunction<? super K, String, ? extends Collection<R>> parse;

        private SearchCollector(Function<? super K, ? extends V> valueMapper, BiFunction<? super K, String, Collection<R>> parse) {
            this.valueMapper = valueMapper;
            this.parse = parse;
        }

        @Override
        public Supplier<Map<K, V>> supplier() {
            return HashMap<K, V>::new;
        }

        @Override
        public BiConsumer<Map<K, V>, K> accumulator() {
            return (map, t) -> {
                V value = valueMapper.apply(t);
                if (value != null) map.put(t, value);
            };
        }

        @Override
        public BinaryOperator<Map<K, V>> combiner() {
            return (map1, map2) -> {
                map1.putAll(map2);
                return map1;
            };
        }

        @Override
        public Function<Map<K, V>, Map<K, V>> finisher() {
            return m -> m;
        }

        @Override
        public Set<Collector.Characteristics> characteristics() {
            return EnumSet.of(Collector.Characteristics.IDENTITY_FINISH);
        }
    }
}
