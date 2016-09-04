package org.unipop.elastic.document;

import io.searchbox.action.BulkableAction;
import io.searchbox.core.*;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unipop.elastic.common.ElasticClient;
import org.unipop.query.UniQuery;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.query.aggregation.ReduceQuery;
import org.unipop.query.aggregation.ReduceVertexQuery;
import org.unipop.query.controller.SimpleController;
import org.unipop.query.mutation.AddEdgeQuery;
import org.unipop.query.mutation.AddVertexQuery;
import org.unipop.query.mutation.PropertyQuery;
import org.unipop.query.mutation.RemoveQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.element.ElementSchema;
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

public class DocumentController implements SimpleController, ReduceQuery.ReduceController, ReduceVertexQuery.ReduceVertexController, LocalQuery.LocalController {
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
        Set<? extends DocumentSchema<E>> schemas = getSchemas(uniQuery.getReturnType());
        SearchCollector<DocumentSchema<E>, Search, E> collector = new SearchCollector<>((schema) -> schema.getSearch(uniQuery), (schema, results) -> schema.parseResults(results, uniQuery));
        Map<DocumentSchema<E>, Search> searches = schemas.stream()
                .collect(collector);
        return search(uniQuery, searches, collector);
    }

    @Override
    public Iterator<Edge> search(SearchVertexQuery uniQuery) {
        SearchCollector<DocumentEdgeSchema, Search, Edge> collector = new SearchCollector<>((schema) -> schema.getSearch(uniQuery), (schema, results) -> schema.parseResults(results, uniQuery));
        Map<DocumentEdgeSchema, Search> schemas = edgeSchemas.stream()
                .collect(collector);
        return search(uniQuery, schemas, collector);
    }

    @Override
    public void fetchProperties(DeferredVertexQuery uniQuery) {
        SearchCollector<DocumentVertexSchema, Search, Vertex> collector = new SearchCollector<>((schema) -> schema.getSearch(uniQuery), (schema, results) -> schema.parseResults(results, uniQuery));
        Map<DocumentVertexSchema, Search> schemas = vertexSchemas.stream()
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
    public Iterator<Object> reduce(ReduceQuery query) {
        SearchCollector<DocumentSchema<Element>, Search, Object> collector = new SearchCollector<>((schema) -> schema.getReduce(query), (schema, results) -> schema.parseReduce(results, query));
        Set<? extends DocumentSchema<Element>> schemas = getSchemas(query.getReturnType());
        Map<DocumentSchema<Element>, Search> searches = schemas.stream().collect(collector);
        Iterator<Object> search = search(query, searches, collector);
        if (search.hasNext())
            return search;
        else
            return null;
    }


    @Override
    public Iterator<Object> reduce(ReduceVertexQuery query) {
        if (!query.isReturnsVertex()){
            SearchCollector<DocumentSchema<Edge>, Search, Object> collector = new SearchCollector<>((schema) -> schema.getReduce(query), (schema, results) -> schema.parseReduce(results, query));
            Set<? extends DocumentSchema<Edge>> schemas = getSchemas(Edge.class);
            Map<DocumentSchema<Edge>, Search> searches = schemas.stream().collect(collector);
            Iterator<Object> search = search(query, searches, collector);
            if (search.hasNext())
                return search;
        } else {
            Iterator<Edge> search = search(new SearchVertexQuery(Edge.class, query.getVertices(), query.getDirection(),
                    PredicatesHolderFactory.empty(), query.getLimit(),
                    Collections.emptySet(), Collections.emptyList(), query.getStepDescriptor()));
            List<Vertex> vertexList = ConversionUtils.asStream(search).flatMap(edge -> {
                List<Vertex> vertices = new ArrayList<>();
                if (query.getDirection().equals(Direction.OUT) || query.getDirection().equals(Direction.BOTH))
                    vertices.add(edge.inVertex());
                if (query.getDirection().equals(Direction.IN) || query.getDirection().equals(Direction.BOTH))
                    vertices.add(edge.outVertex());
                return vertices.stream();
            }).collect(Collectors.toList());
            Set<? extends DocumentSchema<Vertex>> schemas = getSchemas(Vertex.class);
            SearchCollector<DocumentSchema<Vertex>, Search, Object> collector = new SearchCollector<>((schema) -> {
                ReduceQuery reduceQuery = new ReduceQuery(vertexList, query.getPredicates(), query.getPropertyKeys(), query.getReduceOn(),
                        query.getOp(), query.getReturnType(), query.getLimit(), query.getStepDescriptor());
                return schema.getReduce(reduceQuery);
            }, (schema, results) -> schema.parseReduce(results, query));
            Map<DocumentSchema<Vertex>, Search> reduces = schemas.stream().collect(collector);
            return search(query, reduces, collector);
        }
        return null;
    }

    @Override
    public <S extends Element> Iterator<Map<String, S>> local(LocalQuery<S> query) {
        SearchCollector<DocumentSchema<Element>, Search, Object> collector = new SearchCollector<>((schema) -> schema.getLocal(query), (schema, results) -> schema.parseLocal(results, query));
        Set<? extends DocumentSchema<Element>> schemas = getSchemas(query.getQueryClass());
        Map<DocumentSchema<Element>, Search> searches = schemas.stream().collect(collector);
        Iterator<Object> search = search(query, searches, collector);
        return ConversionUtils.asStream(search).map(o -> ((Map<String, S>) o)).iterator();
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

    private void fillChildren(List<MutableMetrics> childMetrics, List<MultiSearchResult.MultiSearchResponse> responses) {
        for (int i = 0; i < responses.size(); i++) {
            if (childMetrics.size() > i) {
                MutableMetrics child = childMetrics.get(i);
                MultiSearchResult.MultiSearchResponse response = responses.get(i);
                child.setCount(TraversalMetrics.ELEMENT_COUNT_ID, response.searchResult.getTotal());
                child.setDuration(Long.parseLong(response.searchResult.getJsonObject().get("took").toString()), TimeUnit.MILLISECONDS);
            }
        }
    }

    private <E extends Element, S extends DocumentSchema<E>, R> Iterator<R> search(UniQuery query, Map<S, Search> schemas, SearchCollector<S, Search, R> collector) {
        MetricsRunner metrics = new MetricsRunner(this, query,
                schemas.keySet().stream().map(s -> ((ElementSchema) s)).collect(Collectors.toList()));

        if (schemas.size() == 0) return EmptyIterator.instance();
        logger.debug("Preparing search. Schemas: {}", schemas);

        client.refresh();
        MultiSearch multiSearch = new MultiSearch.Builder(schemas.values()).build();
        MultiSearchResult results = client.execute(multiSearch);
        List<MultiSearchResult.MultiSearchResponse> responses = results.getResponses();
        metrics.stop((children) -> fillChildren(children, responses));

        if (results == null || !results.isSucceeded()) return EmptyIterator.instance();
        Iterator<S> schemaIterator = schemas.keySet().iterator();

        return responses.stream().filter(this::valid).flatMap(result ->
                collector.parse.apply(schemaIterator.next(), result.searchResult.getJsonString()).stream()).iterator();
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
