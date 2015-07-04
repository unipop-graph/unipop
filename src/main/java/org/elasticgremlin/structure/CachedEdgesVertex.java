package org.elasticgremlin.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.queryhandler.Predicates;
import org.elasticgremlin.queryhandler.elasticsearch.helpers.RevisionHolder;

import java.util.*;
import java.util.function.Function;

public abstract class CachedEdgesVertex extends BaseVertex implements Vertex {

    private final RevisionHolder revisionHolder;
    private final int revision;
    private HashMap<EdgeQueryInfo, List<Edge>> queriedEdges = new HashMap<>();

    protected CachedEdgesVertex(Object id, String label, ElasticGraph graph, Object[] keyValues, RevisionHolder revisionHolder) {
        super(id, label, graph, keyValues);
        this.revisionHolder = revisionHolder;
        this.revision = revisionHolder.getRevision();
    }

    public Iterator<Edge> edges(Direction direction, String[] edgeLabels, Predicates predicates) {
        List<Edge> edges = queriedEdges.get(new EdgeQueryInfo(direction, edgeLabels, predicates, revisionHolder.getRevision()));
        if (edges != null) {
            return edges.iterator();
        }
        return super.edges(direction, edgeLabels, predicates);
    }

    public void addQueriedEdges(List<Edge> edges, Direction direction, String[] edgeLabels, Predicates predicates) {
        EdgeQueryInfo queryInfo = new EdgeQueryInfo(direction, edgeLabels, predicates, revisionHolder.getRevision());
        queriedEdges.put(queryInfo, edges);
    }

    public void removeEdge(Edge edge) {
        queriedEdges.forEach((edgeQueryInfo, edges) -> edges.remove(edge));
    }

    private static class EdgeQueryInfo {
        private Direction direction;
        private String[] edgeLabels;
        private Predicates predicates;
        private int revision;

        public EdgeQueryInfo(Direction direction, String[] edgeLabels, Predicates predicates, int revision) {
            this.direction = direction;
            this.edgeLabels = edgeLabels;
            this.predicates = predicates;
            this.revision = revision;
        }

        public Direction getDirection() {
            return direction;
        }

        public String[] getEdgeLabels() {
            return edgeLabels;
        }

        public Predicates getPredicates() {
            return predicates;
        }

        // region equals and hashCode

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            EdgeQueryInfo that = (EdgeQueryInfo) o;

            if (revision != that.revision) return false;
            if (direction != that.direction) return false;
            if (!Arrays.equals(edgeLabels, that.edgeLabels)) return false;
            if (!predicates.equals(that.predicates)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = direction.hashCode();
            result = 31 * result + Arrays.hashCode(edgeLabels);
            result = 31 * result + predicates.hashCode();
            result = 31 * result + revision;
            return result;
        }


        // endregion
    }

    public static List<Vertex> getVerticesBulk(Vertex vertex) {
        List<Vertex> vertices = new ArrayList<>();
        if (BaseVertex.class.isAssignableFrom(vertex.getClass())) {
            BaseVertex baseVertex = (BaseVertex) vertex;
            List<Vertex> siblings = baseVertex.getSiblings();
            if (siblings == null || siblings.isEmpty()) {
                vertices.add(vertex);
            }
            else {
                siblings.forEach(vertices::add);
            }
        }
        else {
            vertices.add(vertex);
        }

        return vertices;
    }

    public static Map<Object, List<Edge>> handleBulkEdgeResults(Iterator<Edge> edges, List<Vertex> vertices,
                                                                Direction direction, String[] edgeLabels,
                                                                Predicates predicates) {
        Map<Direction, Function<Edge, Object[]>> directionToIdFunc = new HashMap<Direction, Function<Edge, Object[]>>() {{
            put(Direction.IN, edge -> new Object[]{edge.inVertex().id()});
            put(Direction.OUT, edge -> new Object[]{edge.outVertex().id()});
            put(Direction.BOTH, edge -> new Object[]{edge.inVertex().id(), edge.outVertex().id()});
        }};

        Map<Object, List<Edge>> idToEdges = new HashMap<>();
        edges.forEachRemaining(edge -> {
            Object[] vertexIds = directionToIdFunc.get(direction).apply(edge);
            for (Object vertexId : vertexIds) {
                addEdgeToMap(idToEdges, edge, vertexId);
            }
        });

        List<CachedEdgesVertex> baseVertices = extractBaseVertices(vertices);

        baseVertices.forEach(vertex -> {
            List<Edge> vertexEdges = idToEdges.get(vertex.id());
            if (vertexEdges != null) {
                vertex.addQueriedEdges(vertexEdges, direction, edgeLabels, predicates);
            }
            else {
                vertexEdges = new ArrayList<>(0);
                idToEdges.put(vertex.id(), vertexEdges);
                vertex.addQueriedEdges(vertexEdges, direction, edgeLabels, predicates);
            }
        });

        return idToEdges;
    }

    private static List<CachedEdgesVertex> extractBaseVertices(List<Vertex> vertices) {
        List<CachedEdgesVertex> baseVertices = new ArrayList<>();
        vertices.forEach(vertex -> {
            if (BaseVertex.class.isAssignableFrom(vertex.getClass())) {
                baseVertices.add((CachedEdgesVertex) vertex);
            }
        });
        return baseVertices;
    }

    private static void addEdgeToMap(Map<Object, List<Edge>> idToEdges, final Edge edge, Object vertexId) {
        List<Edge> edges = idToEdges.get(vertexId);

        if (edges == null) {
            edges = new ArrayList<>();
            idToEdges.put(vertexId, edges);
        }
        edges.add(edge);
    }
}
