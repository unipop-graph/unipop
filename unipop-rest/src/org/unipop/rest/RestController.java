package org.unipop.rest;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.BaseRequest;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unipop.query.UniQuery;
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
import org.unipop.structure.traversalfilter.TraversalFilter;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 24/11/16.
 */
public class RestController implements SimpleController {
    private static final Logger logger = LoggerFactory.getLogger(RestController.class);

    private final UniGraph graph;

    private Set<? extends RestVertexSchema> vertexSchemas = new HashSet<>();
    private Set<? extends RestEdgeSchema> edgeSchemas = new HashSet<>();

    TraversalFilter traversalFilter;

    public RestController(UniGraph graph, Set<RestSchema> schemas, TraversalFilter traversalFilter) {

        this.traversalFilter = traversalFilter;
        this.graph = graph;
        Set<RestSchema> documentSchemas = collectSchemas(schemas);
        this.vertexSchemas = documentSchemas.stream().filter(schema -> schema instanceof RestVertexSchema)
                .map(schema -> ((RestVertexSchema) schema)).collect(Collectors.toSet());
        this.edgeSchemas = documentSchemas.stream().filter(schema -> schema instanceof RestEdgeSchema)
                .map(schema -> ((RestEdgeSchema) schema)).collect(Collectors.toSet());

        logger.debug("Instantiated RestController: {}", this);
    }

    private Set<RestSchema> collectSchemas(Set<? extends ElementSchema> schemas) {
        Set<RestSchema> docSchemas = new HashSet<>();

        schemas.forEach(schema -> {
            if (schema instanceof RestSchema) {
                docSchemas.add((RestSchema) schema);
                Set<RestSchema> childSchemas = collectSchemas(schema.getChildSchemas());
                docSchemas.addAll(childSchemas);
            }
        });
        return docSchemas;
    }

    @Override
    public Iterator<Edge> search(SearchVertexQuery uniQuery) {
        RestCollector<RestEdgeSchema, BaseRequest, Edge> collector =
                new RestCollector<>(schema -> schema.getSearch(uniQuery),
                        (schema, result) -> schema.parseResults(result, uniQuery));

        Map<RestEdgeSchema, BaseRequest> schemas = edgeSchemas.stream()
                .filter(schema -> this.traversalFilter.filter(schema, uniQuery.getTraversal())).collect(collector);

        return search(uniQuery, schemas, collector);
    }

    @Override
    public <E extends Element> Iterator<E> search(SearchQuery<E> uniQuery) {
        RestCollector<RestSchema<E>, BaseRequest, E> collector =
                new RestCollector<>(schema -> schema.getSearch(uniQuery),
                        (schema, result) -> schema.parseResults(result, uniQuery));
        Set<? extends RestSchema<E>> schemas = getSchemas(uniQuery.getReturnType());
        Map<RestSchema<E>, BaseRequest> collect = schemas.stream()
                .filter(schema -> this.traversalFilter.filter(schema,uniQuery.getTraversal())).collect(collector);
        return search(uniQuery, collect, collector);
    }


    @Override
    public void fetchProperties(DeferredVertexQuery uniQuery) {
        RestCollector<RestVertexSchema, BaseRequest, Vertex> collector =
                new RestCollector<>(schema -> schema.getSearch(uniQuery),
                        (schema, result) -> schema.parseResults(result, uniQuery));
        Map<RestVertexSchema, BaseRequest> schemas = vertexSchemas.stream()
                .filter(schema -> this.traversalFilter.filter(schema,uniQuery.getTraversal())).collect(collector);
        Iterator<Vertex> iterator = search(uniQuery, schemas, collector);
        Map<Object, DeferredVertex> vertexMap = uniQuery.getVertices().stream()
                .collect(Collectors.toMap(UniElement::id, Function.identity(), (a, b) -> a));
        iterator.forEachRemaining(newVertex -> {
            DeferredVertex deferredVertex = vertexMap.get(newVertex.id());
            if (deferredVertex != null) deferredVertex.loadProperties(newVertex);
        });
    }

    @Override
    public <E extends Element> void remove(RemoveQuery<E> uniQuery) {
        uniQuery.getElements().forEach(element -> {
            Set<? extends RestSchema<Element>> schemas = getSchemas(element.getClass());
            for (RestSchema<Element> schema : schemas) {
                BaseRequest delete = schema.delete(element);
                try {
                    HttpResponse<JsonNode> jsonNodeHttpResponse = delete.asJson();
                    if(jsonNodeHttpResponse.getStatus() == 200)
                        break;
                } catch (UnirestException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Override
    public Vertex addVertex(AddVertexQuery uniQuery) {
        UniVertex vertex = new UniVertex(uniQuery.getProperties(), null, graph);
        for (RestVertexSchema vertexSchema : vertexSchemas) {
            try {
                BaseRequest baseRequest = vertexSchema.addElement(vertex);
                if (baseRequest == null)
                    return vertex;
                baseRequest.asJson();
            } catch (UnirestException e) {
                e.printStackTrace();
            } catch (NoSuchElementException e) {
                continue;
            }
        }
        return vertex;
    }

    @Override
    public <E extends Element> void property(PropertyQuery<E> uniQuery) {
        E element = uniQuery.getElement();
        Set<? extends RestSchema<E>> schemas = getSchemas(element.getClass());
        for (RestSchema<E> schema : schemas) {
            try {
                BaseRequest baseRequest = schema.addElement(element);
                if (baseRequest == null)
                    return;
                baseRequest.asJson();
            } catch (UnirestException e) {
                e.printStackTrace();
            } catch (NoSuchElementException e) {
                continue;
            }
        }
    }

    private <E extends Element> Set<? extends RestSchema<E>> getSchemas(Class elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass))
            return (Set<? extends RestSchema<E>>) vertexSchemas;
        else return (Set<? extends RestSchema<E>>) edgeSchemas;
    }

    @Override
    public Edge addEdge(AddEdgeQuery uniQuery) {
        UniEdge edge = new UniEdge(uniQuery.getProperties(), uniQuery.getOutVertex(), uniQuery.getInVertex(), null,
                graph);
        for (RestEdgeSchema edgeSchema : edgeSchemas) {
            try {
                BaseRequest baseRequest = edgeSchema.addElement(edge);
                if (baseRequest == null)
                    return edge;
                baseRequest.asJson();
            } catch (UnirestException e) {
                e.printStackTrace();
            } catch (NoSuchElementException e) {
                continue;
            }
        }
        return edge;
    }

    private <E extends Element, S extends RestSchema<E>> Iterator<E> search(UniQuery query, Map<S, BaseRequest> schemas, RestCollector<S, BaseRequest, E> collector) {

        if (schemas.size() == 0) return EmptyIterator.instance();
        logger.debug("Preparing search. Schemas: {}", schemas);

        List<HttpResponse<JsonNode>> results = schemas.values().stream().map(request -> {
            try {
                return request.asJson();
            } catch (UnirestException e) {
                throw new RuntimeException("request: " + request.toString() + " unsuccessful");
            }
        }).collect(Collectors.toList());

        Iterator<S> schemaIterator = schemas.keySet().iterator();

        return results.stream().flatMap(result ->
                collector.parse.apply(schemaIterator.next(), result).stream()).iterator();
    }

    public class RestCollector<K, V, R> implements Collector<K, Map<K, V>, Map<K, V>> {
        private final Function<? super K, ? extends V> valueMapper;
        private final BiFunction<? super K, HttpResponse<JsonNode>, ? extends Collection<R>> parse;

        private RestCollector(Function<? super K, ? extends V> valueMapper, BiFunction<? super K, HttpResponse<JsonNode>, Collection<R>> parse) {
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
