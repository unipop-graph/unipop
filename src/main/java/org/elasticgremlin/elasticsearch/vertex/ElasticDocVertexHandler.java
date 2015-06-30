package org.elasticgremlin.elasticsearch.vertex;

import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.elasticsearch.*;
import org.elasticgremlin.elasticsearch.edge.ElasticDocEdge;
import org.elasticgremlin.querying.*;
import org.elasticgremlin.structure.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;

import java.util.*;

public class ElasticDocVertexHandler implements VertexHandler {

    private ElasticGraph graph;
    private Client client;
    private ElasticMutations elasticMutations;
    private String indexName;
    private final int scrollSize;
    private final boolean refresh;

    private LazyGetter lazyGetter;

    public ElasticDocVertexHandler(ElasticGraph graph, Client client, ElasticMutations elasticMutations, String indexName, int scrollSize, boolean refresh) {
        this.graph = graph;
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.indexName = indexName;
        this.scrollSize = scrollSize;
        this.refresh = refresh;
    }

    @Override
    public Iterator<Vertex> vertices() {
        return new SearchQuery<>(indexName, FilterBuilders.missingFilter(ElasticDocEdge.InId),
                0, scrollSize, Integer.MAX_VALUE, client, this::createVertex, refresh);
    }

    @Override
    public Iterator<Vertex> vertices(Object[] vertexIds) {
        List<Vertex> vertices = new ArrayList<>();
        for(Object id : vertexIds)
            vertices.add(new ElasticDocVertex(id, null, null, graph, getLazyGetter(), elasticMutations, indexName));
        return vertices.iterator();
    }

    @Override
    public Iterator<Vertex> vertices(Predicates predicates) {
        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        boolFilter.must(FilterBuilders.missingFilter(ElasticDocEdge.InId));
        return new SearchQuery<>(indexName, boolFilter, 0, scrollSize,
                predicates.limitHigh - predicates.limitLow, client, this::createVertex, refresh);
    }

    @Override
    public Vertex vertex(Object vertexId, String vertexLabel, Edge edge, Direction direction) {
        return new ElasticDocVertex(vertexId,vertexLabel, null ,graph,getLazyGetter(), elasticMutations, indexName);
    }

    @Override
    public Vertex addVertex(Object id, String label, Object[] properties) {
        Vertex v = new ElasticDocVertex(id, label, properties, graph, null, elasticMutations, indexName);

        try {
            elasticMutations.addElement(v, indexName, null, true);
        } catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.vertexWithIdAlreadyExists(id);
        }
        return v;
    }

    private LazyGetter getLazyGetter() {
        if(lazyGetter == null || !lazyGetter.canRegister())
            lazyGetter = new LazyGetter(client, indexName);
        return lazyGetter;
    }

    private Vertex createVertex(SearchHit hit) {
        BaseVertex vertex = new ElasticDocVertex(hit.id(), hit.getType(), null, graph, null, elasticMutations, indexName);
        hit.getSource().entrySet().forEach((field) -> vertex.addPropertyLocal(field.getKey(), field.getValue()));
        return vertex;
    }
}
