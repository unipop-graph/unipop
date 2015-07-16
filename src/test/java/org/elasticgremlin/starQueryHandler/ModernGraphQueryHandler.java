package org.elasticgremlin.starQueryHandler;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.queryhandler.*;
import org.elasticgremlin.queryhandler.elasticsearch.helpers.*;
import org.elasticgremlin.queryhandler.elasticsearch.stardoc.*;
import org.elasticgremlin.queryhandler.elasticsearch.vertexdoc.DocVertexHandler;
import org.elasticgremlin.structure.*;
import org.elasticsearch.client.Client;

import java.io.IOException;
import java.util.*;

public class ModernGraphQueryHandler implements QueryHandler {

    private static final String PERSON = "person";
    private static final String SOFTWARE = "software";

    private StarHandler starHandler;
    private DocVertexHandler docVertexHandler;
    private Client client;
    private Map<String, VertexHandler> vertexHandlers;
    private ElasticMutations elasticMutations;
    private TimingAccessor timing;

    @Override
    public void init(ElasticGraph graph, Configuration configuration) throws IOException {
        String indexName = configuration.getString("elasticsearch.index.name", "graph");
        boolean refresh = configuration.getBoolean("elasticsearch.refresh", false);
        int scrollSize = configuration.getInt("elasticsearch.scrollSize", 100);

        this.client = ElasticClientFactory.create(configuration);
        ElasticHelper.createIndex(indexName, client);

        timing = new TimingAccessor();
        elasticMutations = new ElasticMutations(false, client, timing);
        this.docVertexHandler = new DocVertexHandler(graph, client, elasticMutations, indexName, scrollSize, refresh, timing);
        this.starHandler = new StarHandler(graph, client, elasticMutations, indexName, scrollSize, refresh, timing,
                new BasicEdgeMapping("knows", "person", Direction.OUT, "knows-fk"), new BasicEdgeMapping("created", "software", Direction.OUT, "created-fk"));

        this.vertexHandlers = new HashMap<>();
        this.vertexHandlers.put(PERSON, starHandler);
        this.vertexHandlers.put(SOFTWARE, docVertexHandler);
    }

    @Override
    public void commit() { elasticMutations.commit(); }

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
    public Map<Object, List<Edge>> edges(Iterator<BaseVertex> vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        return starHandler.edges(vertices, direction, edgeLabels, predicates);
    }

    @Override
    public Edge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {
        return starHandler.addEdge(edgeId, label, outV, inV, properties);
    }

    @Override
    public Iterator<? extends Vertex> vertices() {
        final Iterator<Vertex> starVertices = (Iterator<Vertex>) starHandler.vertices();
        final Iterator<Vertex> docVertices = docVertexHandler.vertices();

        return new ConcatIterator<>(starVertices, docVertices);
    }

    @Override
    public Iterator<? extends Vertex> vertices(Object[] vertexIds) {
        final Iterator<Vertex> starVertices = (Iterator<Vertex>) starHandler.vertices(vertexIds);
        final Iterator<Vertex> docVertices = (Iterator<Vertex>) docVertexHandler.vertices(vertexIds);

        return new ConcatIterator<>(starVertices, docVertices);
    }

    @Override
    public Iterator<? extends Vertex> vertices(Predicates predicates) {
        String label = extractLabel(predicates.hasContainers);
        if (label == null) {
            Iterator<? extends Vertex> vertices = vertices();
            return testPredicatesLocal(predicates, vertices);
        }

        return vertexHandlers.get(label).vertices(predicates);
    }


    @Override
    public BaseVertex vertex(Object vertexId, String vertexLabel, Edge edge, Direction direction) {
        if (vertexLabel == null) {
            BaseVertex starVertex = starHandler.vertex(vertexId, vertexLabel, edge, direction);
            BaseVertex docVertex = docVertexHandler.vertex(vertexId, vertexLabel, edge, direction);
            return starVertex == null ? docVertex : starVertex ;
        }

        return vertexHandlers.get(vertexLabel).vertex(vertexId, vertexLabel, edge, direction);
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Object[] properties) {
        return vertexHandlers.get(label).addVertex(id, label, properties);
    }

    @Override
    public void printStats() {
        timing.print();
    }

    @Override
    public void close() {
        client.close();
    }

    private Iterator<Vertex> testPredicatesLocal(Predicates predicates, Iterator<? extends Vertex> vertices) {
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
