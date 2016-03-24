package org.unipop.elastic.controllers;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.unipop.controller.*;
import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.QueryIterator;
import org.unipop.elastic.schema.ElementSchema;
import org.unipop.elastic.schema.Filter;
import org.unipop.elastic.schema.SchemaManager;
import org.unipop.structure.BaseVertex;

import java.util.*;

public class DocumentController implements QueryController, MutateController, VertexController, EdgeController {

    private final Client client;
    private final ElasticMutations elasticMutations;
    private final SchemaSet<Vertex> vertexSchemas;
    private final SchemaSet<Edge> edgeSchemas;
    private SchemaManager schemaManager;

    public DocumentController(Client client, ElasticMutations elasticMutations, SchemaManager schemaManager) {
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.schemaManager = schemaManager;
        this.vertexSchemas = new SchemaSet<>(schemaManager.getSchemas(ElementSchema.class, Vertex.class));
        this.edgeSchemas = new SchemaSet<>(schemaManager.getSchemas(ElementSchema.class, Edge.class));
    }

    @Override
    public <E extends Element> Iterator<E> query(Predicates<E> predicates) {
        SchemaSet<E> schemaSet = (SchemaSet<E>) (predicates.getElementType().equals(Vertex.class) ? vertexSchemas : edgeSchemas);
        Set<ElementSchema<E>> schemas = schemaSet.getElementSchemas();
        return getQueryIterator(predicates, schemas);
    }

    private <E extends Element> Iterator<E> getQueryIterator(Predicates predicates, Set<ElementSchema<E>> schemas) {
        Set<Filter> filters = new HashSet<>();
        Set<ElementSchema<E>> relevantSchemas = new HashSet<>();
        for(ElementSchema<E> schema : schemas) {
            Filter newFilter = schema.getFilter(predicates);
            if(newFilter == null) continue;
            relevantSchemas.add(schema);
            boolean merged = false;
            for(Filter filter : filters) merged |= filter.merge(newFilter);
            if(!merged) filters.add(newFilter);
        }
        if(filters.size() == 0) return EmptyIterator.instance();

        BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();
        filters.forEach(filter -> boolFilter.should(filter.getFilterBuilder()));
        QueryBuilder query = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolFilter);
        String[] indices = filters.stream().<String>flatMap(filter -> filter.getIndices().stream()).toArray(String[]::new);
        elasticMutations.refresh(indices);
        QueryIterator.Parser<E> parser = (hit) -> relevantSchemas.stream().map(schema -> schema.createElement(hit, this)).findFirst().get();
        return new QueryIterator<>(query, 0, predicates.getLimitHigh(), client, parser, null, indices);
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
    public Iterator<Edge> edges(BaseVertex vertex, Direction direction, String[] edgeLabels) {
        return get
    }

    @Override
    public <E extends Element> Iterator<Traverser<E>> edges(List<Traverser.Admin<Vertex>> vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        return null;
    }

    @Override
    public Edge addEdge(Object edgeId, String label, BaseVertex outV, BaseVertex inV, Map<String, Object> properties) {
        return null;
    }

    @Override
    public BaseVertex vertex(Direction direction, Object vertexId, String vertexLabel) {
        return null;
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Map<String, Object> properties) {
        return null;
    }

}
