package org.unipop.elastic.document;

import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.apache.commons.collections.map.HashedMap;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unipop.common.util.SchemaSet;
import org.unipop.elastic.common.ElasticClient;
import org.unipop.elastic.common.FilterHelper;
import org.unipop.elastic.document.schema.DocEdgeSchema;
import org.unipop.elastic.document.schema.DocSchema;
import org.unipop.elastic.document.schema.DocVertexSchema;
import org.unipop.query.StepDescriptor;
import org.unipop.query.UniQuery;
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
import org.unipop.schema.reference.DeferredVertex;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.io.IOException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DocumentController implements SimpleController {

    private ElasticClient client;
    private Set<? extends DocVertexSchema> vertexSchemas;
    private Set<? extends DocEdgeSchema> edgeSchemas;
    private UniGraph graph;

    private final Logger logger;

    public DocumentController(ElasticClient client, SchemaSet schemas, UniGraph graph) {
        this.client = client;
        this.vertexSchemas = schemas.get(DocVertexSchema.class, true);
        this.edgeSchemas = schemas.get(DocEdgeSchema.class, true);
        this.graph = graph;

        this.logger = LoggerFactory.getLogger(DocumentController.class);

        Iterator<String> indices = schemas.get(DocSchema.class, true).stream().map(DocSchema::getIndex).distinct().iterator();
        client.validateIndex(indices);

    }

    //region QueryController
    @Override
    public <E extends Element> Iterator<E> search(SearchQuery<E> uniQuery) {
        Set<? extends DocSchema<E>> schemas = getSchemas(uniQuery.getReturnType());
        BiFunction<SearchQuery, DocSchema<E>, PredicatesHolder> toPredicatesFunction = (SearchQuery q, DocSchema<E> schema) -> schema.toPredicates(q.getPredicates());
        this.logger.debug("executing search with parameters -> SearchQuery: {}, schemas: {}, limit: {}, stepDescriptor: {}",
                uniQuery,
                schemas,
                uniQuery.getLimit(),
                uniQuery.getStepDescriptor()
        );

        Iterator<E> resultIterator = search(schemas, uniQuery, toPredicatesFunction, uniQuery.getLimit(), uniQuery.getStepDescriptor());
        this.logger.info("executed search with SearchQuery: {}, resultIterator: {}", uniQuery, resultIterator);
        return resultIterator;
    }

    @Override
    public Iterator<Edge> search(SearchVertexQuery uniQuery) {
        BiFunction<SearchVertexQuery, DocEdgeSchema, PredicatesHolder> toPredicatesFunction = (SearchVertexQuery q, DocEdgeSchema schema) ->
                schema.toPredicates(uniQuery.getPredicates(), uniQuery.gertVertices(), uniQuery.getDirection());
        this.logger.debug("executing search with SearchVertexQuery: {}", uniQuery);
        Iterator<Edge> edgeIterator = search(edgeSchemas, uniQuery, toPredicatesFunction, uniQuery.getLimit(), uniQuery.getStepDescriptor());
        this.logger.info("executed search with SearchVertexQuery: {}, resultEdgeIterator: {}", uniQuery, edgeIterator);
        return edgeIterator;
    }

    @Override
    public void fetchProperties(DeferredVertexQuery uniQuery) {
        BiFunction<DeferredVertexQuery, DocVertexSchema, PredicatesHolder> toPredicatesFunction = (DeferredVertexQuery q, DocVertexSchema schema) ->
                schema.toPredicates(uniQuery.getVertices());
        int limit = -1;
        this.logger.debug("executing fetch properties with DeferredVertexQuery: {}, VertexSchema: {}, limit: {}",
                uniQuery,
                vertexSchemas,
                limit
        );
        Iterator<Vertex> search = search(vertexSchemas, uniQuery, toPredicatesFunction, limit, uniQuery.getStepDescriptor());
        this.logger.debug("executed search to fetch properties, result: {}", search);
        Map<Object, DeferredVertex> vertexMap = uniQuery.getVertices().stream().collect(Collectors.toMap(UniElement::id, Function.identity(), (a, b) -> a));
        this.logger.debug("created vertex map of deferred vertices, map: {}", vertexMap);
        search.forEachRemaining(newVertex -> {
            DeferredVertex deferredVertex = vertexMap.get(newVertex.id());
            this.logger.debug("loading deferred vertex if not null with properties, deferredVertex: {}, newVertex: {}",
                    deferredVertex,
                    newVertex
            );
            if (deferredVertex != null) {
                deferredVertex.loadProperties(newVertex);
                this.logger.debug("loaded deferred vertex with properties, filled deferred vertex: {}", deferredVertex);
            }
        });

        this.logger.info("fetched properties and loaded vertices, filled vertices map: {}", vertexMap);
    }

    @Override
    public Edge addEdge(AddEdgeQuery uniQuery) {
        this.logger.debug("executing edge addition, AddEdgeQuery: {}", uniQuery);
        UniEdge edge = new UniEdge(uniQuery.getProperties(), uniQuery.getOutVertex(), uniQuery.getInVertex(), graph);
        this.logger.debug("indexing edge with parameters: edgeSchemas: {}, UniEdge: {}", edgeSchemas, edge);
        try {
            index(this.edgeSchemas, edge);
        } catch (DocumentAlreadyExistsException ex) {
            this.logger.warn("failed to index edge, elastic document already exists throwing edgeWithIdAlreadyExists", ex);
            throw Graph.Exceptions.edgeWithIdAlreadyExists(edge.id());
        }
        this.logger.info("executed addEdge successfully, edge added to graph, edge: {}", edge);
        return edge;
    }

    @Override
    public Vertex addVertex(AddVertexQuery uniQuery) {
        this.logger.debug("executing vertex addition, AddVertexQuery: {}", uniQuery);
        UniVertex vertex = new UniVertex(uniQuery.getProperties(), graph);
        this.logger.debug("indexing vertex with parameters: vertexSchemas: {}, UniVertex: {}", this.vertexSchemas, vertex);
        try {
            index(this.vertexSchemas, vertex);
        } catch (DocumentAlreadyExistsException ex) {
            this.logger.warn("failed to index vertex, elastic document already exists throwing vertexWithIdAlreadyExists", ex);
            throw Graph.Exceptions.vertexWithIdAlreadyExists(vertex.id());
        }
        this.logger.info("executed addVertex successfully, vertex added to graph, vertex: {}", vertex);
        return vertex;
    }

    @Override
    public <E extends Element> void property(PropertyQuery<E> uniQuery) {
        Set<? extends DocSchema<E>> schemas = getSchemas(uniQuery.getElement().getClass());
        this.logger.debug("executing PropertyQuery with parameters: schemas: {}, PropertyQuery: {}", schemas, uniQuery);

        try {
            index(schemas, uniQuery.getElement());
        } catch (DocumentAlreadyExistsException ex) {
            this.logger.warn("failed to update document by PropertyQuery", ex);
        }
        this.logger.info("successfully managed to update element with PropertyQuery, element: {}", uniQuery.getElement());
    }

    @Override
    public <E extends Element> void remove(RemoveQuery<E> uniQuery) {
        this.logger.debug("executing RemoveQuery, query: {}", uniQuery);
        uniQuery.getElements().forEach(element -> {
            Set<? extends DocSchema<Element>> schemas = getSchemas(element.getClass());
            this.logger.debug("deleting element by schemas, schemas: {}, element: {}", schemas, element);
            delete(schemas, element);
        });
    }
    //endregion

    //region Helpers
    private <E extends Element> Set<? extends DocSchema<E>> getSchemas(Class elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass))
            return (Set<? extends DocSchema<E>>) vertexSchemas;
        else return (Set<? extends DocSchema<E>>) edgeSchemas;
    }
    //endregion

    //region Elastic Queries

    private <E extends Element, S extends DocSchema<E>, Q extends UniQuery> Iterator<E> search(Set<? extends S> allSchemas, Q uniQuery, BiFunction<Q, S, PredicatesHolder> toPredicatesFunction, int limit, StepDescriptor stepDescriptor) {
        Map<S, PredicatesHolder> predicatesMap = new HashedMap();
        for (S schema : allSchemas) {
            PredicatesHolder schemaPredicates = toPredicatesFunction.apply(uniQuery, schema);
            if (!schemaPredicates.isAborted()) predicatesMap.put(schema, schemaPredicates);
        }
        if (predicatesMap.size() == 0) return EmptyIterator.instance();
        PredicatesHolder predicates = PredicatesHolderFactory.or(predicatesMap.values());
        Set<S> schemas = predicatesMap.keySet();

        client.refresh();
        QueryBuilder filterBuilder = FilterHelper.createFilterBuilder(predicates);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(filterBuilder).fetchSource(true)
                .size(limit == -1 ? 10000 : limit);
        Search.Builder search = new Search.Builder(searchSourceBuilder.toString());
        schemas.forEach(schema -> search.addIndex(schema.getIndex()));

        SearchResult result = client.execute(search.build());
        if (!result.isSucceeded()) return EmptyIterator.instance();
        return parseResults(schemas, result.getJsonString());
    }

    ObjectMapper mapper = new ObjectMapper();

    private <E extends Element, S extends DocSchema<E>> Iterator<E> parseResults(Set<S> schemas, String result) {
        List<E> results = new ArrayList<>();
        try {
            JsonNode hits = mapper.readTree(result).get("hits").get("hits");
            for (JsonNode hit : hits) {
                Map<String, Object> source = mapper.readValue(hit.get("_source").toString(), Map.class);
                DocSchema.Document document = new DocSchema.Document(hit.get("_index").asText(), hit.get("_type").asText(), hit.get("_id").asText(), source);
                for (S schema : schemas) {
                    E element = schema.fromDocument(document);
                    if (element != null) results.add(element);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return results.iterator();
    }

    private <E extends Element> void index(Set<? extends DocSchema<E>> schemas, E element) {
        for (DocSchema<E> schema : schemas) {
            DocSchema.Document document = schema.toDocument(element);
            if (document != null) {
                Index index = new Index.Builder(document.getFields()).index(document.getIndex()).type(document.getType()).id(document.getId()).build();
                client.bulk(index);
            }
        }
    }

    private <E extends Element> void delete(Set<? extends DocSchema<E>> schemas, E element) {
        for (DocSchema<E> schema : schemas) {
            DocSchema.Document document = schema.toDocument(element);
            if (document != null) {
                Delete build = new Delete.Builder(document.getId()).index(document.getIndex()).type(document.getType()).build();
                client.bulk(build);
            }
        }
    }

    //endregion
}
