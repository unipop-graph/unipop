package org.unipop.starQueryHandler;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.controller.*;
import org.unipop.controller.elasticsearch.helpers.*;
import org.unipop.controller.elasticsearch.star.*;
import org.unipop.controller.elasticsearch.vertex.VertexController;
import org.unipop.controllerprovider.ControllerProvider;
import org.unipop.structure.*;
import org.elasticsearch.client.Client;

import java.io.IOException;
import java.util.*;

public class ModernGraphControllerProvider implements ControllerProvider {

    private static final String PERSON = "person";
    private static final String SOFTWARE = "software";

    private StarController starHandler;
    private VertexController docVertexHandler;
    private Client client;
    private Map<String, org.unipop.controller.VertexController> vertexHandlers;
    private ElasticMutations elasticMutations;
    private TimingAccessor timing;

    @Override
    public void init(UniGraph graph, Configuration configuration) throws IOException {
        String indexName = configuration.getString("elasticsearch.index.name", "graph");
        boolean refresh = configuration.getBoolean("elasticsearch.refresh", false);
        int scrollSize = configuration.getInt("elasticsearch.scrollSize", 100);

        this.client = ElasticClientFactory.create(configuration);
        ElasticHelper.createIndex(indexName, client);

        timing = new TimingAccessor();
        elasticMutations = new ElasticMutations(false, client, timing);
        this.docVertexHandler = new VertexController(graph, client, elasticMutations, indexName, scrollSize, refresh, timing);
        this.starHandler = new StarController(graph, client, elasticMutations, indexName, scrollSize, refresh, timing,
                new BasicEdgeMapping("knows", "person", Direction.OUT, "knows-fk"), new BasicEdgeMapping("created", "software", Direction.OUT, "created-fk"));

        this.vertexHandlers = new HashMap<>();
        this.vertexHandlers.put(PERSON, starHandler);
        this.vertexHandlers.put(SOFTWARE, docVertexHandler);
    }

    @Override
    public void commit() { elasticMutations.commit(); }


    @Override
    public EdgeController getEdgeHandler(Predicates predicates) {
        return null;
    }

    @Override
    public EdgeController getEdgeHandler(Vertex vertex, Direction direction, String[] edgeLabels, Predicates predicates) {
        return null;
    }

    @Override
    public EdgeController addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {
        return null;
    }

    @Override
    public void printStats() {
        timing.print();
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public org.unipop.controller.VertexController getVertexHandler(Object[] ids) {
        return null;
    }

    @Override
    public org.unipop.controller.VertexController getVertexHandler(Predicates predicates) {
        return null;
    }

    @Override
    public org.unipop.controller.VertexController getVertexHandler(Object vertexId, String vertexLabel, Edge edge, Direction direction) {
        return null;
    }

    @Override
    public org.unipop.controller.VertexController addVertex(Object id, String label, Object[] properties) {
        return null;
    }

    @Override
    public EdgeController getEdgeHandler(Object[] ids) {
        return null;
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
