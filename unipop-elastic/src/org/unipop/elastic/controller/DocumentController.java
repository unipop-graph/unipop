package org.unipop.elastic.controller;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.unipop.common.schema.SchemaSet;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.QueryIterator;
import org.unipop.elastic.schema.*;
import org.unipop.query.controller.SimpleController;
import org.unipop.query.mutation.AddEdgeQuery;
import org.unipop.query.mutation.AddVertexQuery;
import org.unipop.query.mutation.PropertyQuery;
import org.unipop.query.mutation.RemoveQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.query.search.SearchVertexQuery;

import java.util.*;

public class DocumentController implements SimpleController {

    private final Client client;
    private final ElasticMutations elasticMutations;
    private final SchemaSet<ElasticVertexSchema> vertexSchemas;
    private final SchemaSet<ElasticEdgeSchema> edgeSchemas;

    public DocumentController(Client client, ElasticMutations elasticMutations, SchemaSet<ElasticElementSchema> schemaSet) {
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.vertexSchemas = schemaSet.getSchemas(ElasticVertexSchema.class);
        this.edgeSchemas = schemaSet.getSchemas(ElasticEdgeSchema.class);
    }

    @Override
    public <E extends Element>  Iterator<E> query(SearchQuery<E> uniQuery) {
        Set<Filter> filters = new HashSet<>();
        SchemaSet<ElasticElementSchema<E>> elasticElementSchemas = (SchemaSet<ElasticElementSchema<E>>)(uniQuery.getReturnType().isAssignableFrom(Vertex.class) ? vertexSchemas : edgeSchemas);
        Set<ElasticElementSchema<E>> relevantSchemas = new HashSet<>();
        for(ElasticElementSchema<E> schema : elasticElementSchemas) {
            Filter newFilter = schema.getFilter(uniQuery);
            if(newFilter == null) continue;
            relevantSchemas.add(schema);
            boolean merged = false;
            for(Filter filter : filters) merged |= filter.merge(newFilter);
            if(!merged) filters.add(newFilter);
        }
        if(filters.size() == 0) return EmptyIterator.instance();
        QueryIterator.Parser<E> parser = (hit) -> relevantSchemas.stream().map(schema -> schema.fromFields(hit)).findFirst().get();
        return getQueryIterator(filters, parser, 0);
    }

    @Override
    public Iterator<Edge> query(SearchVertexQuery uniQuery) {
        Set<Filter> filters = new HashSet<>();
        Set<ElasticEdgeSchema> relevantSchemas = new HashSet<>();
        for(ElasticEdgeSchema schema : edgeSchemas) {
            Filter newFilter = schema.getFilter(uniQuery);
            if(newFilter == null) continue;
            relevantSchemas.add(schema);
            boolean merged = false;
            for(Filter filter : filters) merged |= filter.merge(newFilter);
            if(!merged) filters.add(newFilter);
        }
        if(filters.size() == 0) return EmptyIterator.instance();
        QueryIterator.Parser<Edge> parser = (hit) -> relevantSchemas.stream().map(schema -> schema.fromFields(hit)).findFirst().get();
        return getQueryIterator(filters, parser, 0);
    }


    private <E extends Element> Iterator<E> getQueryIterator(Set<Filter> filters, QueryIterator.Parser<E> parser, int limit) {
        BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();
        filters.forEach(filter -> boolFilter.should(filter.getFilterBuilder()));
        QueryBuilder query = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolFilter);
        String[] indices = filters.stream().<String>flatMap(filter -> filter.getIndices().stream()).toArray(String[]::new);
        elasticMutations.refresh(indices);
        return new QueryIterator<>(query, 0, limit, client, parser, null, indices);
    }


    @Override
    public Edge addEdge(AddEdgeQuery uniQuery) {
        Edge edge = edgeSchemas.stream().map(schema -> schema.createEdge(uniQuery)).findFirst().get();
        try {
            elasticMutations.addElement(edge, true);
        } catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.vertexWithIdAlreadyExists(edge.id());
        }
        return edge;
    }

    @Override
    public Vertex addVertex(AddVertexQuery uniQuery) {
        Vertex vertex = vertexSchemas.stream().map(schema -> schema.createVertex(uniQuery)).findFirst().get();
        try {
            elasticMutations.addElement(vertex, true);
        } catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.vertexWithIdAlreadyExists(vertex.id());
        }
        return vertex;
    }

    @Override
    public void property(PropertyQuery uniQuery) {
        elasticMutations.updateElement(uniQuery.getElement(), false);
    }

    @Override
    public void remove(RemoveQuery uniQuery) {
        uniQuery.getElements().forEach(elasticMutations::deleteElement);
    }
}
