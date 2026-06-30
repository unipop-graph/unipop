package org.unipop.elastic.document;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.search.Hit;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unipop.elastic.common.ElasticClient;
import org.unipop.query.controller.SimpleController;
import org.unipop.query.mutation.AddEdgeQuery;
import org.unipop.query.mutation.AddVertexQuery;
import org.unipop.query.mutation.PropertyQuery;
import org.unipop.query.mutation.RemoveQuery;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.element.ElementSchema;
import org.unipop.schema.reference.DeferredVertex;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;
import org.unipop.structure.traversalfilter.TraversalFilter;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;


public class DocumentController implements SimpleController {
    private static final Logger logger = LoggerFactory.getLogger(DocumentController.class);

    private final ElasticClient client;
    private final UniGraph graph;

    private final int maxLimit = 10000;

    private Set<? extends DocumentVertexSchema> vertexSchemas = new HashSet<>();
    private Set<? extends DocumentEdgeSchema> edgeSchemas = new HashSet<>();

    private TraversalFilter traversalFilter;

    public DocumentController(Set<DocumentSchema> schemas, ElasticClient client, UniGraph graph, TraversalFilter traversalFilter) {
        this.client = client;
        this.graph = graph;

        this.traversalFilter = traversalFilter;

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
        Set<? extends DocumentSchema<E>> schemas = getSchemas(uniQuery.getReturnType())
                .stream().filter(schema -> this.traversalFilter.filter(schema, uniQuery.getTraversal()))
                .map(schema -> ((DocumentSchema<E>) schema))
                .collect(Collectors.toSet());
        Map<DocumentSchema<E>, Query> searches = schemas.stream()
                .collect(new SearchCollector<>((schema) -> schema.getSearch(uniQuery)));
        return search(uniQuery, searches);
    }

    @Override
    public Iterator<Edge> search(SearchVertexQuery uniQuery) {
        Map<DocumentEdgeSchema, Query> schemas = edgeSchemas.stream()
                .filter(schema -> this.traversalFilter.filter(schema, uniQuery.getTraversal()))
                .collect(new SearchCollector<>((schema) -> schema.getSearch(uniQuery)));
        return search(uniQuery, schemas);
    }

    @Override
    public void fetchProperties(DeferredVertexQuery uniQuery) {
        Map<DocumentVertexSchema, Query> schemas = vertexSchemas.stream()
                .filter(schema -> this.traversalFilter.filter(schema, uniQuery.getTraversal()))
                .collect(new SearchCollector<>((schema) -> schema.getSearch(uniQuery)));
        Iterator<Vertex> search = search(uniQuery, schemas);

        Map<Object, DeferredVertex> vertexMap = uniQuery.getVertices().stream()
                .collect(Collectors.toMap(UniElement::id, Function.identity(), (a, b) -> a));
        search.forEachRemaining(newVertex -> {
            DeferredVertex deferredVertex = vertexMap.get(newVertex.id());
            if (deferredVertex != null) deferredVertex.loadProperties(newVertex);
        });
    }

    @Override
    public Edge addEdge(AddEdgeQuery uniQuery) {
        UniEdge edge = new UniEdge(uniQuery.getProperties(), uniQuery.getOutVertex(), uniQuery.getInVertex(), null, graph);
        if (index(this.edgeSchemas, edge, true)) return edge;
        return null;
    }

    @Override
    public Vertex addVertex(AddVertexQuery uniQuery) {
        UniVertex vertex = new UniVertex(uniQuery.getProperties(), null, graph);
        if (index(this.vertexSchemas, vertex, true)) return vertex;
        return null;
    }

    @Override
    public <E extends Element> void property(PropertyQuery<E> uniQuery) {
        Set<? extends DocumentSchema<E>> schemas = getSchemas(uniQuery.getElement().getClass());
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

    /**
     * Unchecked cast helper: ES client returns Hit<Map> (raw) from search/scroll,
     * but parseResults expects Hit<Map<String,Object>>. The runtime type is the same;
     * we suppress the warning and cast the list element-by-element into a new list.
     */
    @SuppressWarnings("unchecked")
    private List<Hit<Map<String, Object>>> castHits(List<? extends Hit<?>> raw) {
        List<Hit<Map<String, Object>>> result = new ArrayList<>(raw.size());
        for (Hit<?> h : raw) {
            result.add((Hit<Map<String, Object>>) h);
        }
        return result;
    }

    private <E extends Element, S extends DocumentSchema<E>> Iterator<E> search(SearchQuery<E> query, Map<S, Query> schemas) {
        if (schemas.isEmpty()) return EmptyIterator.instance();
        logger.debug("Preparing search. Schemas: {}", schemas);

        client.refresh();

        List<E> all = new ArrayList<>();
        for (Map.Entry<S, Query> entry : schemas.entrySet()) {
            S schema = entry.getKey();
            List<String> indices = schema.getIndex().getIndex(query.getPredicates());
            SearchRequest request = schema.buildSearch(query, entry.getValue())
                    .index(indices)
                    .scroll(t -> t.time("6m"))
                    .build();

            SearchResponse<Map> response = client.search(request);
            if (response == null) continue;

            List<Hit<Map<String, Object>>> hits = new ArrayList<>(castHits(response.hits().hits()));
            String scrollId = response.scrollId();
            boolean lastBatchWasFull = (response.hits().hits().size() == maxLimit);

            while (scrollId != null
                    && lastBatchWasFull
                    && (query.getLimit() == -1 || hits.size() < query.getLimit())) {
                ScrollResponse<Map> sr = client.scroll(scrollId);
                if (sr == null) break;
                scrollId = sr.scrollId();
                List<Hit<Map<String, Object>>> batch = castHits(sr.hits().hits());
                hits.addAll(batch);
                lastBatchWasFull = (batch.size() == maxLimit);
            }

            if (scrollId != null) client.clearScroll(scrollId);
            all.addAll(schema.parseResults(hits, query));
        }
        return all.iterator();
    }

    private <E extends Element> boolean index(Set<? extends DocumentSchema<E>> schemas, E element, boolean create) {
        for (DocumentSchema<E> schema : schemas) {
            BulkOperation op = schema.addElement(element, create);
            if (op != null) {
                logger.debug("indexing element with schema: {}, element: {}, client: {}", schema, element, client);
                client.bulk(element, op);
                return true;
            }
        }
        return false;
    }

    private <E extends Element> void delete(Set<? extends DocumentSchema<E>> schemas, E element) {
        for (DocumentSchema<E> schema : schemas) {
            BulkOperation op = schema.delete(element);
            if (op != null) {
                logger.debug("deleting element with schema: {}, element: {}, client: {}", schema, element, client);
                client.bulk(element, op);
            }
        }
    }

    //endregion

    private class SearchCollector<K extends DocumentSchema, V extends Query> implements Collector<K, Map<K, V>, Map<K, V>> {

        private final Function<? super K, ? extends V> valueMapper;

        private SearchCollector(Function<? super K, ? extends V> valueMapper) {
            this.valueMapper = valueMapper;
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
