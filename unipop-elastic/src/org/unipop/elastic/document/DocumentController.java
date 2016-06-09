package org.unipop.elastic.document;

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHitField;
import org.unipop.common.schema.referred.DeferredVertex;
import org.unipop.elastic.common.FilterHelper;
import org.unipop.elastic.common.QueryIterator;
import org.unipop.elastic.document.schema.DocEdgeSchema;
import org.unipop.elastic.document.schema.DocSchema;
import org.unipop.elastic.document.schema.DocVertexSchema;
import org.unipop.query.StepDescriptor;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.controller.SimpleController;
import org.unipop.query.mutation.AddEdgeQuery;
import org.unipop.query.mutation.AddVertexQuery;
import org.unipop.query.mutation.PropertyQuery;
import org.unipop.query.mutation.RemoveQuery;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DocumentController implements SimpleController {

    private final Client client;
    private final Set<? extends DocVertexSchema> vertexSchemas;
    private final Set<? extends DocEdgeSchema> edgeSchemas;
    private UniGraph graph;
    private boolean dirty;

    public DocumentController(Client client, Set<DocVertexSchema> vertexSchemas, Set<DocEdgeSchema> edgeSchemas, UniGraph graph) {
        this.client = client;
        this.vertexSchemas = vertexSchemas;
        this.edgeSchemas = edgeSchemas;
        this.graph = graph;
    }

    //region QueryController
    @Override
    public <E extends Element> Iterator<E> search(SearchQuery<E> uniQuery) {
        Set<? extends DocSchema<E>> schemas = getSchemas(uniQuery.getReturnType());
        Set<PredicatesHolder> schemasPredicates = schemas.stream().map(schema ->
                schema.toPredicates(uniQuery.getPredicates())).collect(Collectors.toSet());
        PredicatesHolder schemaPredicateHolders = PredicatesHolderFactory.or(schemasPredicates);
        return search(schemaPredicateHolders, schemas, null, uniQuery.getLimit(), uniQuery.getStepDescriptor());
    }

    @Override
    public Iterator<Edge> search(SearchVertexQuery uniQuery) {
        Set<PredicatesHolder> schemasPredicates = edgeSchemas.stream().map(schema ->
                schema.toPredicates(uniQuery.getPredicates(), uniQuery.gertVertices(), uniQuery.getDirection())).collect(Collectors.toSet());
        PredicatesHolder schemaPredicateHolders = PredicatesHolderFactory.or(schemasPredicates);
        return search(schemaPredicateHolders, edgeSchemas, null, uniQuery.getLimit(), uniQuery.getStepDescriptor());
    }

    @Override
    public void fetchProperties(DeferredVertexQuery uniQuery) {
        Set<PredicatesHolder> schemasPredicates = vertexSchemas.stream().map(schema ->
                schema.toPredicates(uniQuery.getVertices())).collect(Collectors.toSet());
        PredicatesHolder schemaPredicateHolders = PredicatesHolderFactory.or(schemasPredicates);

        if (schemaPredicateHolders.isEmpty()) return;
        Iterator<Vertex> search = search(schemaPredicateHolders, vertexSchemas, uniQuery.getPropertyKeys(), -1, uniQuery.getStepDescriptor());

        Map<Object, List<DeferredVertex>> collect = uniQuery.getVertices().stream().collect(Collectors.groupingBy(UniElement::id));
//        Map<Object, DeferredVertex> vertexMap = uniQuery.getVertices().stream().collect(Collectors.toMap(UniElement::id, Function.identity(), (a, b) -> a));
//        search.forEachRemaining(newVertex -> {
//            DeferredVertex deferredVertex = vertexMap.get(newVertex.id());
//            if (deferredVertex != null) deferredVertex.loadProperties(newVertex);
//        });
        search.forEachRemaining(newVertex -> {
            List<DeferredVertex> deferredVertex = collect.get(newVertex.id());
            if (deferredVertex != null) deferredVertex.forEach(v -> v.loadProperties(newVertex));
        });

    }

    @Override
    public Edge addEdge(AddEdgeQuery uniQuery) {
        UniEdge edge = new UniEdge(uniQuery.getProperties(), uniQuery.getOutVertex(), uniQuery.getInVertex(), graph);
        try {
            index(this.edgeSchemas, edge, true);
        } catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.edgeWithIdAlreadyExists(edge.id());
        }
        return edge;
    }

    @Override
    public Vertex addVertex(AddVertexQuery uniQuery) {
        UniVertex vertex = new UniVertex(uniQuery.getProperties(), graph);
        try {
            index(this.vertexSchemas, vertex, true);
        } catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.vertexWithIdAlreadyExists(vertex.id());
        }
        return vertex;
    }

    @Override
    public <E extends Element> void property(PropertyQuery<E> uniQuery) {
        Set<? extends DocSchema<E>> schemas = getSchemas(uniQuery.getElement().getClass());
        index(schemas, uniQuery.getElement(), false);
    }

    @Override
    public <E extends Element> void remove(RemoveQuery<E> uniQuery) {
        uniQuery.getElements().forEach(element -> {
            Set<? extends DocSchema<Element>> schemas = getSchemas(element.getClass());
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
    private <E extends Element, S extends DocSchema<E>> Iterator<E> search(PredicatesHolder allPredicates, Set<S> schemas, Set<String> propertyKeys, int limit, StepDescriptor stepDescriptor) {
        if (schemas.size() == 0 || allPredicates.isAborted()) return Iterators.emptyIterator();

        FilterBuilder filterBuilder = FilterHelper.createFilterBuilder(allPredicates);
        QueryBuilder query = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filterBuilder);

        String[] indices = schemas.stream().map(DocSchema::getIndex).toArray(String[]::new);
        refresh(indices);
        QueryIterator.Parser<E> parser = (searchHit) -> schemas.stream().map(schema -> {
            if (propertyKeys == null)
                return schema.fromFields(searchHit.getSource());
            Map<String, SearchHitField> fields = searchHit.getFields();
            Map<String, Object> source = new HashMap<>();
            fields.entrySet().forEach(entry -> source.put(entry.getKey(), entry.getValue().getValue()));
            return schema.fromFields(source);
        }).findFirst().get();
        if (propertyKeys == null)
            return new QueryIterator<>(query, stepDescriptor, 0, limit, client, parser, null, indices);
        return new QueryIterator<>(query, stepDescriptor, 0, limit, client, parser, propertyKeys.toArray(new String[propertyKeys.size()]), indices);
    }

    private <E extends Element> IndexResponse index(Set<? extends DocSchema<E>> schemas, E element, boolean create) {
        for (DocSchema<E> schema : schemas) {
            DocSchema.Document document = schema.toDocument(element);
            if (document != null) {
                IndexRequestBuilder indexRequest = client.prepareIndex(document.getIndex(), document.getType(), document.getId())
                        .setSource(document.getFields()).setCreate(create);
                dirty = true;
                return indexRequest.execute().actionGet();
            }
        }
        return null;
    }

    private <E extends Element> DeleteResponse delete(Set<? extends DocSchema<E>> schemas, E element) {
        for (DocSchema<E> schema : schemas) {
            DocSchema.Document document = schema.toDocument(element);
            if (document != null) {
                DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete(document.getIndex(), document.getType(), document.getId());
                dirty = true;
                return deleteRequestBuilder.execute().actionGet();
            }
        }
        return null;
    }

    public void refresh(String... indices) {
        if (dirty) {
            //client.admin().indices().prepareRefresh(indices).execute().actionGet();
            client.admin().indices().prepareRefresh().execute().actionGet();
            dirty = false;
        }
    }
    //endregion
}
