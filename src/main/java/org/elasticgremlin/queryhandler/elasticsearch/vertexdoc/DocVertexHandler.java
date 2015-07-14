package org.elasticgremlin.queryhandler.elasticsearch.vertexdoc;

import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.queryhandler.elasticsearch.helpers.*;
import org.elasticgremlin.queryhandler.elasticsearch.edgedoc.DocEdge;
import org.elasticgremlin.queryhandler.*;
import org.elasticgremlin.structure.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;

import java.util.*;

public class DocVertexHandler implements VertexHandler {

    private ElasticGraph graph;
    private Client client;
    private ElasticMutations elasticMutations;
    private String indexName;
    private final int scrollSize;
    private final boolean refresh;
    private TimingAccessor timing;
    private Map<Direction, LazyGetter> lazyGetters;
    private LazyGetter defaultLazyGetter;

    public DocVertexHandler(ElasticGraph graph, Client client, ElasticMutations elasticMutations, String indexName,
                            int scrollSize, boolean refresh, TimingAccessor timing) {
        this.graph = graph;
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.indexName = indexName;
        this.scrollSize = scrollSize;
        this.refresh = refresh;
        this.timing = timing;
        this.lazyGetters = new HashMap<>();
    }

    @Override
    public Iterator<BaseVertex> vertices() {
        return new QueryIterator<>(FilterBuilders.missingFilter(DocEdge.InId), 0, scrollSize,
                Integer.MAX_VALUE, client, this::createVertex, refresh, timing, indexName);
    }

    @Override
    public Iterator<BaseVertex> vertices(Object[] vertexIds) {
        List<BaseVertex> vertices = new ArrayList<>();
        for(Object id : vertexIds){
            DocVertex vertex = new DocVertex(id, null, null, graph, getLazyGetter(), elasticMutations, indexName);
            vertex.setSiblings(vertices);
            vertices.add(vertex);
        }
        return vertices.iterator();
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates) {
        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        boolFilter.must(FilterBuilders.missingFilter(DocEdge.InId));
        return new QueryIterator<>(boolFilter, 0, scrollSize, predicates.limitHigh - predicates.limitLow,
                client, this::createVertex, refresh, timing, indexName);
    }

    @Override
    public BaseVertex vertex(Object vertexId, String vertexLabel, Edge edge, Direction direction) {
        return new DocVertex(vertexId,vertexLabel, null ,graph,getLazyGetter(direction), elasticMutations, indexName);
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Object[] properties) {
        BaseVertex v = new DocVertex(id, label, properties, graph, null, elasticMutations, indexName);

        try {
            elasticMutations.addElement(v, indexName, null, true);
        } catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.vertexWithIdAlreadyExists(id);
        }
        return v;
    }

    private LazyGetter getLazyGetter() {
        if (defaultLazyGetter == null || !defaultLazyGetter.canRegister()) {
            defaultLazyGetter = new LazyGetter(client, timing);
        }
        return defaultLazyGetter;
    }

    private LazyGetter getLazyGetter(Direction direction) {
        LazyGetter lazyGetter = lazyGetters.get(direction);
        if (lazyGetter == null || !lazyGetter.canRegister()) {
            lazyGetter = new LazyGetter(client, timing);
            lazyGetters.put(direction,
                    lazyGetter);
        }
        return lazyGetter;
    }

    private Iterator<BaseVertex> createVertex(Iterator<SearchHit> hits) {
        ArrayList<BaseVertex> vertices = new ArrayList<>();
        hits.forEachRemaining(hit -> {
            BaseVertex vertex = new DocVertex(hit.id(), hit.getType(), null, graph, null, elasticMutations, indexName);
            vertex.setSiblings(vertices);
            hit.getSource().entrySet().forEach((field) -> vertex.addPropertyLocal(field.getKey(), field.getValue()));
            vertices.add(vertex);
        });
        return vertices.iterator();
    }
}
