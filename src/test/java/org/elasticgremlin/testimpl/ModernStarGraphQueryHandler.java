package org.elasticgremlin.testimpl;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticgremlin.elasticsearch.ElasticClientFactory;
import org.elasticgremlin.elasticsearch.ElasticHelper;
import org.elasticgremlin.elasticsearch.ElasticMutations;
import org.elasticgremlin.queryhandler.Predicates;
import org.elasticgremlin.queryhandler.QueryHandler;
import org.elasticgremlin.queryhandler.VertexHandler;
import org.elasticgremlin.queryhandler.stardoc.BasicEdgeMapping;
import org.elasticgremlin.queryhandler.stardoc.StarHandler;
import org.elasticgremlin.queryhandler.vertexdoc.DocVertexHandler;
import org.elasticgremlin.structure.ElasticGraph;
import org.elasticsearch.client.Client;

import java.io.IOException;
import java.util.*;

public class ModernStarGraphQueryHandler implements QueryHandler {

    private static final String PERSON = "person";
    private static final String SOFTWARE = "software";

    private final StarHandler starHandler;
    private final DocVertexHandler docVertexHandler;
    private final Client client;
    private final String indexName;
    private final ElasticMutations elasticMutations;
    private final boolean refresh;
    private final int scrollSize;

    private Map<String, VertexHandler> vertexHandlers;

    public ModernStarGraphQueryHandler(ElasticGraph graph, Configuration configuration) throws IOException {
        this.indexName = configuration.getString("elasticsearch.index.name", "graph");
        this.refresh = configuration.getBoolean("elasticsearch.refresh", false);
        this.scrollSize = configuration.getInt("elasticsearch.scrollSize", 100);

        this.client = ElasticClientFactory.create(configuration);
        ElasticHelper.createIndex(indexName, client);
        this.elasticMutations = new ElasticMutations(configuration, client);

        this.docVertexHandler = new DocVertexHandler(graph, client, elasticMutations, indexName, scrollSize, refresh);
        this.starHandler = new StarHandler(graph, client, elasticMutations, indexName, scrollSize, refresh,
                new BasicEdgeMapping("knows", "person", Direction.OUT, "knows-fk"),
                new BasicEdgeMapping("created", "software", Direction.OUT, "created-fk"));

        this.vertexHandlers = new HashMap<>();
        this.vertexHandlers.put(PERSON, starHandler);
        this.vertexHandlers.put(SOFTWARE, docVertexHandler);
    }

    @Override
    public Iterator<Edge> edges() {
        return starHandler.edges();
    }

    @Override
    public Iterator<Edge> edges(Object[] edgeIds) {
        return starHandler.edges(edgeIds);
    }

    @Override
    public Iterator<Edge> edges(Predicates predicates) {
        return starHandler.edges(predicates);
    }

    @Override
    public Iterator<Edge> edges(Vertex vertex, Direction direction, String[] edgeLabels, Predicates predicates) {
        return starHandler.edges(vertex, direction, edgeLabels, predicates);
    }

    @Override
    public Edge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {
        return starHandler.addEdge(edgeId, label, outV, inV, properties);
    }

    @Override
    public Iterator<Vertex> vertices() {
        final Iterator<Vertex> starVertices = starHandler.vertices();
        final Iterator<Vertex> docVertices = docVertexHandler.vertices();

        return new ConcatIterator<>(starVertices, docVertices);
    }

    @Override
    public Iterator<Vertex> vertices(Object[] vertexIds) {
        final Iterator<Vertex> starVertices = starHandler.vertices(vertexIds);
        final Iterator<Vertex> docVertices = docVertexHandler.vertices(vertexIds);

        return new ConcatIterator<>(starVertices, docVertices);
    }

    @Override
    public Iterator<Vertex> vertices(Predicates predicates) {
        String label = extractLabel(predicates.hasContainers);
        if (label == null) {
            Iterator<Vertex> vertices = vertices();
            return testPredicatesLocal(predicates, vertices);
        }

        return vertexHandlers.get(label).vertices(predicates);
    }


    @Override
    public Vertex vertex(Object vertexId, String vertexLabel, Edge edge, Direction direction) {
        if (vertexLabel == null) {
            Vertex starVertex = starHandler.vertex(vertexId, vertexLabel, edge, direction);
            Vertex docVertex = docVertexHandler.vertex(vertexId, vertexLabel, edge, direction);
            return starVertex == null ? docVertex : starVertex ;
        }

        return vertexHandlers.get(vertexLabel).vertex(vertexId, vertexLabel, edge, direction);
    }

    @Override
    public Vertex addVertex(Object id, String label, Object[] properties) {
        return vertexHandlers.get(label).addVertex(id, label, properties);
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public void clearAllData() {
        elasticMutations.clearAllData(new String[]{indexName});
    }

    private Iterator<Vertex> testPredicatesLocal(Predicates predicates, Iterator<Vertex> vertices) {
        List<Vertex> passedVertices = new ArrayList<>();
        vertices.forEachRemaining(vertex -> {
            boolean passed = true;
            for (HasContainer has : predicates.hasContainers) {
                passed = passed && has.test(vertex);
            }
            if (passed) {
                passedVertices.add(vertex);
            }
        });
        return passedVertices.iterator();
    }

    private String extractLabel(ArrayList<HasContainer> hasContainers) {
        for (HasContainer hasContainer : hasContainers) {
            if (hasContainer.getKey().equals(T.label.getAccessor())) {
                return hasContainer.getValue().toString();
            }
        }
        return null;
    }

    private static class ConcatIterator<E> implements Iterator<E> {
        private final Iterator<E> firstIterator;
        private final Iterator<E> secondIterator;

        public ConcatIterator(Iterator<E> firstIterator, Iterator<E> secondIterator) {
            this.firstIterator = firstIterator;
            this.secondIterator = secondIterator;
        }

        @Override
        public boolean hasNext() {
            return firstIterator.hasNext() || secondIterator.hasNext();
        }

        @Override
        public E next() {
            return firstIterator.hasNext() ? firstIterator.next() : secondIterator.next();
        }
    }
}
