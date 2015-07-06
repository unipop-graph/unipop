package org.elasticgremlin.queryhandler.elasticsearch.stardoc;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.queryhandler.*;
import org.elasticgremlin.queryhandler.elasticsearch.helpers.*;
import org.elasticgremlin.structure.BaseVertex;
import org.elasticgremlin.structure.ElasticGraph;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;

import java.util.*;

public class StarHandler implements VertexHandler, EdgeHandler {

    private ElasticGraph graph;
    private Client client;
    private ElasticMutations elasticMutations;
    private final int scrollSize;
    private final boolean refresh;
    private TimingAccessor timing;
    private EdgeMapping[] edgeMappings;
    private Map<Direction, LazyGetter> lazyGetters;
    private LazyGetter defaultLazyGetter;

    protected String[] indices;

    public StarHandler(ElasticGraph graph, Client client, ElasticMutations elasticMutations, String indexName,
                       int scrollSize, boolean refresh, TimingAccessor timing, EdgeMapping... edgeMappings) {
        this(graph, client, elasticMutations, new String[] {indexName}, scrollSize, refresh, timing, edgeMappings);
    }

    public StarHandler(ElasticGraph graph, Client client, ElasticMutations elasticMutations, String[] indices,
                       int scrollSize, boolean refresh, TimingAccessor timing, EdgeMapping... edgeMappings) {
        this.graph = graph;
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.indices = indices;
        this.scrollSize = scrollSize;
        this.refresh = refresh;
        this.timing = timing;
        this.edgeMappings = edgeMappings;
        this.lazyGetters = new HashMap<>();
    }

    @Override
    public Iterator<Vertex> vertices() {
        Predicates predicates = new Predicates();
        return vertices(predicates);
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
                client, this::createVertex, refresh, timing, indices);
    }

    @Override
    public Vertex vertex(Object vertexId, String vertexLabel, Edge edge, Direction direction) {
        return new StarVertex(vertexId, vertexLabel, null, graph, getLazyGetter(direction), elasticMutations, getDefaultIndex(), edgeMappings);
    }

    @Override
    public Vertex addVertex(Object id, String label, Object[] properties) {
        String index = getIndex(properties);
        Vertex v = new StarVertex(id, label, properties, graph, null, elasticMutations, index, edgeMappings);

        try {
            elasticMutations.addElement(v, index, null, true);
        } catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.vertexWithIdAlreadyExists(id);
        }
        return v;
    }

    @Override
    public Iterator<Edge> edges() {
        return edges(new Predicates());
    }

    @Override
    public Iterator<Edge> edges(Object[] edgeIds) {
        Predicates predicates = new Predicates();
        predicates.hasContainers.add(new HasContainer(T.id.getAccessor(), P.within(edgeIds)));
        return edges(predicates);
    }

    @Override
    public Iterator<Edge> edges(Predicates predicates) {
        Iterator<Vertex> vertices = vertices();
        List<Edge> edges = new ArrayList<>();
        vertices.forEachRemaining(vertex -> {
            ((BaseVertex) vertex).edges(Direction.IN, new String[0], predicates).forEachRemaining(edges::add);
            ((BaseVertex) vertex).edges(Direction.OUT, new String[0], predicates).forEachRemaining(edges::add);
            ((BaseVertex) vertex).edges(Direction.BOTH, new String[0], predicates).forEachRemaining(edges::add);
        });

        return edges.iterator();
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
                predicates.limitHigh - predicates.limitLow, client, this::createVertex, refresh, timing, indices);

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
        boolean out = shouldContainEdge(outV, Direction.OUT, label, properties);
        boolean in = shouldContainEdge(inV, Direction.IN, label, properties);
        StarVertex containerVertex;
        Vertex otherVertex;
        if (out) {
            containerVertex = (StarVertex) outV;
            otherVertex = inV;
        }
        else if (in) {
            containerVertex = (StarVertex) inV;
            otherVertex = outV;
        }
        else {
            // Neither the in nor the out vertices can contain the edge
            // (Either their mapping is incompatible or they are not of typeStarVertex)
            return null;
        }

        List<Object> keyValues = new ArrayList<>();
        containerVertex.properties().forEachRemaining(property -> {
            keyValues.add(property.key());
            keyValues.add(property.value());
        });

        EdgeMapping mapping = getEdgeMapping(label, out ? Direction.OUT : Direction.IN);
        containerVertex.addInnerEdge(mapping, edgeId, label, otherVertex, keyValues.toArray());

        Predicates predicates = new Predicates();
        predicates.hasContainers.add(new HasContainer(T.id.getAccessor(), P.within(edgeId)));
        return containerVertex.edges(mapping.getDirection(), new String[]{label}, predicates).next();
    }

    private EdgeMapping getEdgeMapping(String label, Direction direction) {
        for (EdgeMapping mapping : edgeMappings) {
            if (mapping.getLabel().equals(label) && mapping.getDirection().equals(direction)) {
                return mapping;
            }
        }
        return null;
    }

    protected boolean shouldContainEdge(Vertex vertex, Direction direction, String edgeLabel, Object[] edgeProperties) {
        if (!StarVertex.class.isAssignableFrom(vertex.getClass())) {
            return false;
        }
        StarVertex starVertex = (StarVertex) vertex;
        EdgeMapping[] mappings = starVertex.getEdgeMappings();
        for (int i = 0; i < mappings.length; i++) {
            EdgeMapping mapping = mappings[i];
            // TODO: Check option of implementing EdgeMapping.equals method
            if (i >= edgeMappings.length || !equals(mapping, edgeMappings[i])) {
                return false;
            }
            if (mapping.getDirection().equals(direction) && mapping.getLabel().equals(edgeLabel)) {
                return true;
            }
        }
        return false;
    }

    protected String getDefaultIndex() {
        return this.indices[0];
    }

    protected String getIndex(Object[] properties) {
        return getDefaultIndex();
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

    private boolean equals(EdgeMapping mapping, EdgeMapping otherMapping) {
        return mapping.getDirection().equals(otherMapping.getDirection()) &&
                mapping.getLabel().equals(otherMapping.getLabel()) &&
                mapping.getExternalVertexField().equals(otherMapping.getExternalVertexField()) &&
                mapping.getExternalVertexLabel().equals(otherMapping.getExternalVertexLabel());
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
