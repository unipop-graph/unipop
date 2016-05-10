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
import org.unipop.common.refer.DeferredVertex;
import org.unipop.elastic.common.FilterHelper;
import org.unipop.elastic.common.QueryIterator;
import org.unipop.elastic.document.schema.DocEdgeSchema;
import org.unipop.elastic.document.schema.DocSchema;
import org.unipop.elastic.document.schema.DocVertexSchema;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.controller.SimpleController;
import org.unipop.query.mutation.AddEdgeQuery;
import org.unipop.query.mutation.AddVertexQuery;
import org.unipop.query.mutation.PropertyQuery;
import org.unipop.query.mutation.RemoveQuery;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.util.*;

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
    public <E extends Element>  Iterator<E> search(SearchQuery<E> uniQuery) {
        Set<? extends DocSchema<E>> schemas = getSchemas(uniQuery.getReturnType());

        PredicatesHolder allPredicates = new PredicatesHolder(PredicatesHolder.Clause.Or);
        Set<DocSchema<E>> relevantSchemas = new HashSet<>();
        for(DocSchema<E> schema : schemas) {
            PredicatesHolder vertexPredicates = schema.toPredicates(uniQuery.getPredicates());
            if(vertexPredicates != null) {
                allPredicates.add(vertexPredicates);
                relevantSchemas.add(schema);
            }
        }
        return search(allPredicates, relevantSchemas, uniQuery.getLimit());
    }

    @Override
    public Iterator<Edge> search(SearchVertexQuery uniQuery) {
        PredicatesHolder allPredicates = new PredicatesHolder(PredicatesHolder.Clause.Or);
        Set<DocEdgeSchema> relevantSchemas = new HashSet<>();
        for(DocEdgeSchema schema : edgeSchemas) {
            PredicatesHolder vertexPredicates = schema.toPredicates(uniQuery.getPredicates(), uniQuery.gertVertices(), uniQuery.getDirection());
            if(vertexPredicates != null) {
                allPredicates.add(vertexPredicates);
                relevantSchemas.add(schema);
            }
        }
        return search(allPredicates, relevantSchemas, uniQuery.getLimit());
    }

    @Override
    public void fetchProperties(DeferredVertexQuery query) {
        PredicatesHolder allPredicates = new PredicatesHolder(PredicatesHolder.Clause.Or);
        Set<DocVertexSchema> relevantSchemas = new HashSet<>();
        for(DocVertexSchema schema : vertexSchemas) {
            PredicatesHolder vertexPredicates = schema.toPredicates(query.gertVertices());
            if(vertexPredicates != null) {
                allPredicates.add(vertexPredicates);
                relevantSchemas.add(schema);
            }
        }
        if(relevantSchemas.size() == 0) return;
        Iterator<Vertex> search = search(allPredicates, relevantSchemas, -1);

        Map<Object, DeferredVertex> vertexMap = new HashMap<>(query.gertVertices().size());
        query.gertVertices().forEach(vertex -> vertexMap.put(vertex.id(), vertex));
        search.forEachRemaining(newVertex -> {
            DeferredVertex deferredVertex = vertexMap.get(newVertex.id());
            if(deferredVertex != null) deferredVertex.loadProperties(UniElement.fullProperties(newVertex));
        });

    }

    @Override
    public Edge addEdge(AddEdgeQuery uniQuery) {
        UniEdge edge = new UniEdge(uniQuery.getProperties(), uniQuery.getOutVertex(), uniQuery.getInVertex(), graph);
        try {
            index(this.edgeSchemas, edge, true);
        }
        catch(DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.edgeWithIdAlreadyExists(edge.id());
        }
        return edge;
    }

    @Override
    public Vertex addVertex(AddVertexQuery uniQuery) {
        UniVertex vertex = new UniVertex(uniQuery.getProperties(), graph);
        try {
            index(this.vertexSchemas, vertex, true);
        }
        catch(DocumentAlreadyExistsException ex){
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
        if(Vertex.class.isAssignableFrom(elementClass))
            return (Set<? extends DocSchema<E>>) vertexSchemas;
        else return (Set<? extends DocSchema<E>>) edgeSchemas;
    }
    //endregion

    //region Elastic Queries
    private <E extends Element, S extends DocSchema<E>> Iterator<E> search(PredicatesHolder allPredicates, Set<S> schemas, int limit) {
        if(schemas.size() == 0) return Iterators.emptyIterator();
        QueryBuilder query;
        if(!allPredicates.isEmpty()) {
            FilterBuilder filterBuilder = FilterHelper.createFilterBuilder(allPredicates);
            query = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filterBuilder);
        } else query = QueryBuilders.matchAllQuery();
        String[] indices = schemas.stream().map(DocSchema::getIndex).toArray(String[]::new);
        refresh(indices);
        QueryIterator.Parser<E> parser = (searchHit) -> schemas.stream().map(schema -> schema.fromFields(searchHit.getSource())).findFirst().get();
        return new QueryIterator<>(query, 0, limit, client, parser, indices);
    }

    private <E extends Element> IndexResponse index(Set<? extends DocSchema<E>> schemas, E element, boolean create) {
        for(DocSchema<E> schema : schemas) {
            Map<String, Object> fields = schema.toFields(element);
            if (fields != null) {
                String id = fields.get("id").toString();
                String type = fields.get("type").toString();
                IndexRequestBuilder indexRequest = client.prepareIndex(schema.getIndex(), type, id)
                        .setSource(fields).setCreate(create);
                dirty = true;
                IndexResponse indexResponse = indexRequest.execute().actionGet();
                return indexResponse;
            }
        }
        return null;
    }

    private <E extends Element> DeleteResponse delete(Set<? extends DocSchema<E>> schemas, E element) {
        for(DocSchema<E> schema : schemas) {
            Map<String, Object> fields = schema.toFields(element);
            if (fields != null) {
                Object id = fields.get("id");
                String type = schema.getType();
                if (type.equals("")){
                    type = fields.get("type").toString();
                }
                if (element instanceof Vertex){
                    ((Vertex) element).edges(Direction.BOTH).forEachRemaining(Element::remove);
                }
                DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete(schema.getIndex(), type, id.toString());
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
