package org.unipop.elastic.document;

import io.searchbox.action.BulkableAction;
import io.searchbox.core.*;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unipop.query.predicates.PredicatesHolder;
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

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.unipop.common.util.LogHelper.formatCollection;

public class DocumentController implements SimpleController {
    private static final Logger logger = LoggerFactory.getLogger(DocumentController.class);

    private final ElasticClient client;
    private final UniGraph graph;

    private Set<? extends DocumentVertexSchema> vertexSchemas = new HashSet<>();
    private Set<? extends DocumentEdgeSchema> edgeSchemas = new HashSet<>();

    public DocumentController(Set<DocumentSchema> schemas, ElasticClient client, UniGraph graph) {
        this.client = client;
        this.graph = graph;
        logger.debug("instantiated logger with ElasticClient: {}, UniGraph: {}. Schemas: {}",
                this.client,
                this.graph,
                formatCollection(schemas));

        extractDocumentSchemas(schemas);
    }

    //region Query Controller
    @Override
    public <E extends Element>  Iterator<E> search(SearchQuery<E> uniQuery) {
        Set<? extends DocumentSchema<E>> schemas = getSchemas(uniQuery.getReturnType());
        Function<DocumentSchema<E>, PredicatesHolder> toPredicatesFunction = (schema) -> schema.toPredicates(uniQuery.getPredicates());
        logger.debug(
                "executing search with parameters, SearchQuery: {}, Appropiate DocumentSchemas: {}, toPredicatesFunction: {}",
                uniQuery,
                schemas,
                toPredicatesFunction
                );
        Iterator<E> resultIterator = search(schemas, uniQuery, toPredicatesFunction);
        logger.info("execute search with parameters, SearchQuery: {}, Appropriate DocumentSchemas: {}, returned resultIterator: {}",
                uniQuery,
                schemas,
                resultIterator
        );
        return resultIterator;
    }

    @Override
    public Iterator<Edge> search(SearchVertexQuery uniQuery) {
        Function<DocumentEdgeSchema, PredicatesHolder> toPredicatesFunction = (schema) -> schema.toPredicates(uniQuery.getVertices(), uniQuery.getDirection(), uniQuery.getPredicates());
        return search(edgeSchemas, uniQuery, toPredicatesFunction);
    }

    @Override
    public void fetchProperties(DeferredVertexQuery uniQuery) {
        Function<DocumentVertexSchema, PredicatesHolder> toPredicatesFunction = (schema) -> schema.toPredicates(uniQuery.getVertices());
        Iterator<Vertex> search = search(vertexSchemas, uniQuery, toPredicatesFunction);

        Map<Object, DeferredVertex> vertexMap = uniQuery.getVertices().stream()
                .collect(Collectors.toMap(UniElement::id, Function.identity(), (a, b) -> a));
        search.forEachRemaining(newVertex -> {
            DeferredVertex deferredVertex = vertexMap.get(newVertex.id());
            if(deferredVertex != null) deferredVertex.loadProperties(newVertex);
        });
    }

    @Override
    public Edge addEdge(AddEdgeQuery uniQuery) {
        UniEdge edge = new UniEdge(uniQuery.getProperties(), uniQuery.getOutVertex(), uniQuery.getInVertex(), graph);
        try {
            logger.debug("attempting to index. edge: {}, edgeSchemas: {}", edge, this.edgeSchemas);
            index(this.edgeSchemas, edge);
        }
        catch(DocumentAlreadyExistsException ex) {
            logger.info("Document already exists in elastic", ex);
            throw Graph.Exceptions.edgeWithIdAlreadyExists(edge.id());
        }
        return edge;
    }

    @Override
    public Vertex addVertex(AddVertexQuery uniQuery) {
        UniVertex vertex = new UniVertex(uniQuery.getProperties(), graph);
        try {
            index(this.vertexSchemas, vertex);
        }
        catch(DocumentAlreadyExistsException ex){
            logger.info("Document already exists in elastic", ex);
            throw Graph.Exceptions.vertexWithIdAlreadyExists(vertex.id());
        }
        return vertex;
    }

    @Override
    public <E extends Element> void property(PropertyQuery<E> uniQuery) {
        Set<? extends DocumentSchema<E>> schemas = getSchemas(uniQuery.getElement().getClass());
        //updates
        index(schemas, uniQuery.getElement());
    }

    @Override
    public <E extends Element> void remove(RemoveQuery<E> uniQuery) {
        uniQuery.getElements().forEach(element -> {
            Set<? extends DocumentSchema<Element>> schemas = getSchemas(element.getClass());
            delete(schemas, element);
        });
    }

    private <E extends Element> Set<? extends DocumentSchema<E>> getSchemas(Class elementClass) {
        if(Vertex.class.isAssignableFrom(elementClass))
            return (Set<? extends DocumentSchema<E>>) vertexSchemas;
        else return (Set<? extends DocumentSchema<E>>) edgeSchemas;
    }
    //endregion

    //region Elastic Queries

    private <E extends Element, S extends DocumentSchema<E>> Iterator<E> search(Set<? extends S> allSchemas,
                                                                                   SearchQuery<E> query,
                                                                                   Function<S, PredicatesHolder> toSearchFunction) {
        Map<S, Search> schemas = allSchemas.stream()
                .map(schema -> Tuple.tuple(schema, toSearchFunction.apply(schema)))
                .filter(tuple -> tuple.v2() != null)
                .map(tuple -> Tuple.tuple(tuple.v1(), tuple.v1().getSearch(query, tuple.v2())))
                .filter(tuple -> tuple.v2() != null)
                .collect(Collectors.toMap(Tuple::v1, Tuple::v2));

        logger.debug("mapped schemas for search, schemas: {}", schemas);
        if(schemas.size() == 0) {
            logger.warn("schemas are empty, returning empty iterator");
            return EmptyIterator.instance();
        }
        logger.debug("refreshing client: {}", client);
        client.refresh();
        MultiSearch multiSearch = new MultiSearch.Builder(schemas.values()).build();
        logger.debug("built multiSearch: {}", multiSearch);
        MultiSearchResult results = client.execute(multiSearch);
        logger.debug("results of multiSearch, results: {}", results);
        if(!results.isSucceeded()) {
            logger.warn("results failed to execute, returning empty iterator. results: {}", results);
            return EmptyIterator.instance();
        }
        Iterator<S> schemaIterator = schemas.keySet().iterator();
        logger.debug("executed multiSearch successfully, validating results and parsing with schemas. results: {}, schemaIterator: {}", results, schemaIterator);
        return results.getResponses().stream().filter(this::valid).flatMap(result ->
                schemaIterator.next().parseResults(result.searchResult.getJsonString(), query).stream()).iterator();
    }

    private boolean valid(MultiSearchResult.MultiSearchResponse multiSearchResponse) {
        if(multiSearchResponse.isError) {
            logger.error("failed to execute multiSearch: {}", multiSearchResponse);
            return false;
        }
        logger.debug("multiSearchResult is valid, multiSearch: {}", multiSearchResponse);
        return true;
    }

    private <E extends Element> void index(Set<? extends DocumentSchema<E>> schemas, E element) {
        logger.debug("indexing element, schemas: {}, element: {}", schemas, element);
        for(DocumentSchema<E> schema : schemas) {
            BulkableAction<DocumentResult> index = schema.addElement(element);

            if(index != null) {
                logger.debug("adding element to bulk for schema: {}, element: {}, index: {}, client: {}", schema, element, index, client);
                client.bulk(index);
            }
        }
    }

    private <E extends Element> void delete(Set<? extends DocumentSchema<E>> schemas, E element) {
        logger.debug("deleting element, schemas: {}, element: {}", schemas, element);
        for(DocumentSchema<E> schema : schemas) {
            Delete.Builder delete = schema.delete(element);
            if(delete != null) {
                logger.debug("deleting schema, DeleteBuilder: {}, client: {}", delete, client);
                client.bulk(delete.build());
            }
        }
    }

    //endregion

    //region private helpers
    private void extractDocumentSchemas(Set<DocumentSchema> schemas) {
        logger.debug("extracting document schemas to element schemas, DocumentSchemas: {}", schemas);
        Set<DocumentSchema> documentSchemas = collectSchemas(schemas);
        this.vertexSchemas = documentSchemas.stream().filter(schema -> schema instanceof DocumentVertexSchema)
                .map(schema -> ((DocumentVertexSchema)schema)).collect(Collectors.toSet());
        this.edgeSchemas = documentSchemas.stream().filter(schema -> schema instanceof DocumentEdgeSchema)
                .map(schema -> ((DocumentEdgeSchema)schema)).collect(Collectors.toSet());
        logger.info("extraced document schemas, vertexSchemas: {}, edgeSchemas: {}", this.vertexSchemas, this.edgeSchemas);
    }

    private Set<DocumentSchema> collectSchemas(Set<? extends ElementSchema> schemas) {
        Set<DocumentSchema> docSchemas = new HashSet<>();

        schemas.forEach(schema -> {
            if(schema instanceof DocumentSchema) {
                docSchemas.add((DocumentSchema) schema);
                Set<DocumentSchema> childSchemas = collectSchemas(schema.getChildSchemas());
                docSchemas.addAll(childSchemas);
            }
        });
        return docSchemas;
    }
    //endregion
}
