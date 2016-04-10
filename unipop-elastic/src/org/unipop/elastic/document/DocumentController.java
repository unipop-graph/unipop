package org.unipop.elastic.document;

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.unipop.common.schema.SchemaSet;
import org.unipop.elastic.common.QueryIterator;
import org.unipop.elastic.document.schema.DocSchema;
import org.unipop.query.controller.SimpleController;
import org.unipop.query.mutation.AddEdgeQuery;
import org.unipop.query.mutation.AddVertexQuery;
import org.unipop.query.mutation.PropertyQuery;
import org.unipop.query.mutation.RemoveQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.util.*;

public class DocumentController implements SimpleController {

    private final Client client;
    private final SchemaSet<DocSchema<Vertex>> vertexSchemas;
    private final SchemaSet<DocSchema<Edge>> edgeSchemas;
    private UniGraph graph;
    private boolean dirty;

    public DocumentController(Client client, SchemaSet<DocSchema<Vertex>> vertexSchemas, SchemaSet<DocSchema<Edge>> edgeSchemas, UniGraph graph) {
        this.client = client;
        this.vertexSchemas = vertexSchemas;
        this.edgeSchemas = edgeSchemas;
        this.graph = graph;
    }

    //region QueryController
    @Override
    public <E extends Element>  Iterator<E> search(SearchQuery<E> uniQuery) {
        SchemaSet<? extends DocSchema<E>> schemas = getSchemas(uniQuery.getReturnType());
        Map<DocSchema<E>, List<HasContainer>> predicatesMap = getPredicates(uniQuery.getPredicates(), schemas);
        if(predicatesMap.size() == 0) return Iterators.emptyIterator();
        return search(predicatesMap.values(), predicatesMap.keySet(), uniQuery.getLimit());
    }

    @Override
    public Iterator<Edge> search(SearchVertexQuery uniQuery) {
        Map<DocSchema<Edge>, List<HasContainer>> predicatesMap = getPredicates(uniQuery.getPredicates(), edgeSchemas);
        if(predicatesMap.size() == 0) return Iterators.emptyIterator();
        Collection<List<HasContainer>> predicates = predicatesMap.values();
        //TODO: add vertices to predicates
        return search(predicates, predicatesMap.keySet(), uniQuery.getLimit());
    }

    @Override
    public Edge addEdge(AddEdgeQuery uniQuery) {
        UniEdge edge = new UniEdge(uniQuery.getProperties(), uniQuery.getOutVertex(), uniQuery.getInVertex(), graph);
        index(this.edgeSchemas, edge, true);
        return edge;
    }

    @Override
    public Vertex addVertex(AddVertexQuery uniQuery) {
        UniVertex vertex = new UniVertex(uniQuery.getProperties(), graph);
        index(this.vertexSchemas, vertex, true);
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
    private <E extends Element> SchemaSet<? extends DocSchema<E>> getSchemas(Class elementClass) {
        if(Vertex.class.isAssignableFrom(elementClass))
            return (SchemaSet<? extends DocSchema<E>>) vertexSchemas;
        else return (SchemaSet<? extends DocSchema<E>>) edgeSchemas;
    }

    private <E extends Element> Map<DocSchema<E>, List<HasContainer>> getPredicates(List<HasContainer> predicates, SchemaSet<? extends  DocSchema<E>> schemas) {
        Map<DocSchema<E>, List<HasContainer>> results = new HashMap<>();
        for(DocSchema<E> schema : schemas) {
            List<HasContainer> schemaPredicates = schema.toPredicates(predicates);
            if(schemaPredicates != null) results.put(schema, schemaPredicates);
        }
        return results;
    }
    //endregion

    //region Elastic Queries
    private <E extends Element> Iterator<E> search(Collection<List<HasContainer>> predicatesList, Set<DocSchema<E>> relevantSchemas, int limit) {
        QueryBuilder query = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter);
        String[] indices = relevantSchemas.stream().map(DocSchema::getIndex).toArray(String[]::new);
        refresh(indices);
        QueryIterator.Parser<E> parser = (searchHit) -> relevantSchemas.stream().map(schema -> schema.fromFields(searchHit.getSource())).findFirst().get();
        return new QueryIterator<>(query, 0, limit, client, parser, null, indices);
    }

    private <E extends Element> IndexResponse index(Set<? extends DocSchema<E>> schemas, E element, boolean create) {
        for(DocSchema<E> schema : schemas) {
            Map<String, Object> fields = schema.toFields(element);
            if (fields != null) {
                Object id = fields.get("_id");
                IndexRequestBuilder indexRequest = client.prepareIndex(schema.getIndex(), schema.getType(), id.toString())
                        .setSource(fields).setCreate(create);
                dirty = true;
                return indexRequest.execute().actionGet();
            }
        }
        return null;
    }

    private <E extends Element> DeleteResponse delete(Set<? extends DocSchema<E>> schemas, E element) {
        for(DocSchema<E> schema : schemas) {
            Map<String, Object> fields = schema.toFields(element);
            if (fields != null) {
                Object id = fields.get("_id");
                DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete(schema.getIndex(), schema.getType(), id.toString());
                dirty = true;
                return deleteRequestBuilder.execute().actionGet();
            }
        }
        return null;
    }

    public void refresh(String... indices) {
        if (dirty) {
            client.admin().indices().prepareRefresh(indices).execute().actionGet();
            dirty = false;
        }
    }
    //endregion
}
