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
import org.unipop.controller.*;
import org.unipop.elastic.controller.helpers.ElasticMutations;
import org.unipop.elastic.controller.helpers.QueryIterator;
import org.unipop.elastic.schema.*;
import org.unipop.structure.BaseVertex;

import java.util.*;

public class DocumentController implements VertexController, EdgeController {

    private final Client client;
    private final ElasticMutations elasticMutations;
    private final SchemaSet<ElasticVertexSchema> vertexSchemas;
    private final SchemaSet<ElasticEdgeSchema> edgeSchemas;
    private final SchemaSet<ElasticElementSchema> schemaSet;

    public DocumentController(Client client, ElasticMutations elasticMutations, SchemaSet<ElasticElementSchema> schemaSet) {
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.schemaSet = schemaSet;
        this.vertexSchemas = schemaSet.getSchemas(ElasticVertexSchema.class);
        this.edgeSchemas = schemaSet.getSchemas(ElasticEdgeSchema.class);
    }

    @Override
    public <E extends Element> Iterator<E> query(Predicates<E> predicates) {
        Set<Filter> filters = new HashSet<>();
        Set<ElasticElementSchema<E>> relevantSchemas = new HashSet<>();
        for(ElasticElementSchema schema : schemaSet) {
            Filter newFilter = schema.getFilter(predicates);
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


    private <E extends Element> Iterator<E> getQueryIterator(Set<Filter> filters, QueryIterator.Parser<E> parser, int limit) {
        BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();
        filters.forEach(filter -> boolFilter.should(filter.getFilterBuilder()));
        QueryBuilder query = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolFilter);
        String[] indices = filters.stream().<String>flatMap(filter -> filter.getIndices().stream()).toArray(String[]::new);
        elasticMutations.refresh(indices);
        return new QueryIterator<>(query, 0, limit, client, parser, null, indices);
    }

    @Override
    public void remove(Element element) {
        elasticMutations.deleteElement(element);
    }

    @Override
    public void addProperty(Element element, Property property) {
        elasticMutations.updateElement(element, false);
    }

    @Override
    public void removeProperty(Element element, Property property) {
        elasticMutations.updateElement(element, false);
    }

    @Override
    public Iterator<Edge> edges(List<Vertex> vertices, Direction direction, Predicates<Edge> predicates) {
        Set<Filter> filters = new HashSet<>();
        Set<ElasticEdgeSchema> relevantSchemas = new HashSet<>();
        for(ElasticEdgeSchema schema : edgeSchemas) {
            Filter newFilter = schema.getFilter(vertices, predicates);
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

    @Override
    public Vertex addVertex(Map<String, Object> properties) {
        Vertex vertex = vertexSchemas.stream().map(schema -> schema.createVertex(properties)).findFirst().get();
        try {
            elasticMutations.addElement(vertex, true);
        } catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.vertexWithIdAlreadyExists(vertex.id());
        }
        return vertex;
    }

    @Override
    public Edge addEdge(Vertex outV, Vertex inV, Map<String, Object> properties) {
        Edge edge = edgeSchemas.stream().map(schema -> schema.createEdge(properties, outV, inV)).findFirst().get();
        try {
            elasticMutations.addElement(edge, true);
        } catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.vertexWithIdAlreadyExists(edge.id());
        }
        return edge;
    }
}
