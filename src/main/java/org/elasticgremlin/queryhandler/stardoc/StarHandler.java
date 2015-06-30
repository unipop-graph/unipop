package org.elasticgremlin.queryhandler.stardoc;

import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.elasticsearch.*;
import org.elasticgremlin.queryhandler.*;
import org.elasticgremlin.structure.ElasticGraph;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;

public class StarHandler implements VertexHandler, EdgeHandler {

    private ElasticGraph graph;
    private Client client;
    private ElasticMutations elasticMutations;
    private final int scrollSize;
    private final boolean refresh;
    private EdgeMapping[] edgeMappings;
    private Map<Direction, LazyGetter> lazyGetters;
    private LazyGetter defaultLazyGetter;

    protected String[] indices;

    public StarHandler(ElasticGraph graph, Client client, ElasticMutations elasticMutations, String indexName, int scrollSize, boolean refresh, EdgeMapping... edgeMappings) {
        this.graph = graph;
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.indices = new String[]{indexName};
        this.scrollSize = scrollSize;
        this.refresh = refresh;
        this.edgeMappings = edgeMappings;
        this.lazyGetters = new HashMap<>();
    }

    public StarHandler(ElasticGraph graph, Client client, ElasticMutations elasticMutations, String[] indices, int scrollSize, boolean refresh, EdgeMapping... edgeMappings) {
        this.graph = graph;
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.indices = indices;
        this.scrollSize = scrollSize;
        this.refresh = refresh;
        this.edgeMappings = edgeMappings;
        this.lazyGetters = new HashMap<>();
    }

    @Override
    public Iterator<Vertex> vertices() {
        throw new NotImplementedException();
    }

    @Override
    public Iterator<Vertex> vertices(Object[] vertexIds) {
        List<Vertex> vertices = new ArrayList<>();
        for (Object id : vertexIds) {
            StarVertex vertex = new StarVertex(id, null, null, graph, getLazyGetter(), elasticMutations, getDefaultIndex(), edgeMappings);
            vertex.setSiblings(vertices);
            vertices.add(vertex);
        }
        return vertices.iterator();
    }

    @Override
    public Iterator<Vertex> vertices(Predicates predicates) {
        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        return new QueryIterator<>(boolFilter, 0, scrollSize, predicates.limitHigh - predicates.limitLow,
                client, this::createVertex, refresh, indices);
    }

    @Override
    public Vertex vertex(Object vertexId, String vertexLabel, Edge edge, Direction direction) {
        return new StarVertex(vertexId, vertexLabel, null, graph, getLazyGetter(direction), elasticMutations, getDefaultIndex(), edgeMappings);
    }

    @Override
    public Vertex addVertex(Object id, String label, Object[] properties) {
        throw new NotImplementedException();
    }

    @Override
    public Iterator<Edge> edges() {
        throw new NotImplementedException();
    }

    @Override
    public Iterator<Edge> edges(Object[] edgeIds) {
        throw new NotImplementedException();
    }

    @Override
    public Iterator<Edge> edges(Predicates predicates) {
        throw new NotImplementedException();
    }

    @Override
    public Iterator<Edge> edges(Vertex vertex, Direction direction, String[] edgeLabels, Predicates predicates) {
        List<Vertex> vertices = ElasticHelper.getVerticesBulk(vertex);
        List<Object> vertexIds = new ArrayList<>(vertices.size());
        vertices.forEach(singleVertex -> vertexIds.add(singleVertex.id()));

        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        OrFilterBuilder mappingFilter = FilterBuilders.orFilter();
        boolean empty = true;
        for (EdgeMapping mapping : edgeMappings) {
            if (edgeLabels != null && edgeLabels.length > 0 && !contains(edgeLabels, mapping.getLabel())) continue;
            mappingFilter.add(FilterBuilders.termsFilter(mapping.getExternalVertexField(), vertexIds.toArray()));
            empty = false;
        }
        if (!empty) {
            boolFilter.must(mappingFilter);
        }

        QueryIterator<Vertex> vertexSearchQuery = new QueryIterator<>(boolFilter, 0, scrollSize,
                predicates.limitHigh - predicates.limitLow, client, this::createVertex, refresh, indices);

        Iterator<Edge> edgeResults = new EdgeResults(vertexSearchQuery, direction, edgeLabels);

        Map<Object, List<Edge>> idToEdges = ElasticHelper.handleBulkEdgeResults(edgeResults, vertices,
                direction, edgeLabels, predicates);

        return idToEdges.get(vertex.id()).iterator();
    }

    public static boolean contains(String[] edgeLabels, String label) {
        for (String edgeLabel : edgeLabels)
            if (edgeLabel.equals(label)) return true;
        return false;
    }

    @Override
    public Edge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {
        throw new NotImplementedException();
    }

    protected String getDefaultIndex() {
        return this.indices[0];
    }

    private LazyGetter getLazyGetter() {
        if (defaultLazyGetter == null || !defaultLazyGetter.canRegister()) {
            defaultLazyGetter = new LazyGetter(client);
        }
        return defaultLazyGetter;
    }

    private LazyGetter getLazyGetter(Direction direction) {
        LazyGetter lazyGetter = lazyGetters.get(direction);
        if (lazyGetter == null || !lazyGetter.canRegister()) {
            lazyGetter = new LazyGetter(client);
            lazyGetters.put(direction,
                    lazyGetter);
        }
        return lazyGetter;
    }

    private Iterator<Vertex> createVertex(Iterator<SearchHit> hits) {
        ArrayList<Vertex> vertices = new ArrayList<>();
        hits.forEachRemaining(hit -> {
            StarVertex vertex = new StarVertex(hit.id(), hit.getType(), null, graph, null, elasticMutations, hit.getIndex(), edgeMappings);
            vertex.setFields(hit.getSource());
            vertex.setSiblings(vertices);
            vertices.add(vertex);
        });
        return vertices.iterator();
    }

    private static class EdgeResults implements Iterator<Edge> {

        public Iterator<Edge> edges;

        private Iterator<Vertex> vertexSearchQuery;
        private Direction direction;
        private String[] edgeLabels;

        public EdgeResults(Iterator<Vertex> vertexSearchQuery, Direction direction, String... edgeLabels) {
            this.vertexSearchQuery = vertexSearchQuery;
            this.direction = direction;
            this.edgeLabels = edgeLabels;
        }

        @Override
        public boolean hasNext() {
            return (edges != null && edges.hasNext()) || vertexSearchQuery.hasNext();
        }

        @Override
        public Edge next() {
            if (edges == null || !edges.hasNext())
                edges = vertexSearchQuery.next().edges(direction.opposite(), edgeLabels);
            return edges.next();
        }
    }
}
