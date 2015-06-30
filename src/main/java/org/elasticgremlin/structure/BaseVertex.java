package org.elasticgremlin.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.*;
import org.elasticgremlin.queryhandler.Predicates;
import org.elasticsearch.action.get.MultiGetItemResponse;

import java.util.*;

public abstract class BaseVertex extends BaseElement implements Vertex {

    private List<Vertex> siblings;
    private HashMap<EdgeQueryInfo, List<Edge>> queriedEdges = new HashMap<>();

    protected BaseVertex(Object id, String label, ElasticGraph graph, Object[] keyValues) {
        super(id, label, graph, keyValues);
        this.siblings = null;
    }

    @Override
    public Property createProperty(String key, Object value) {
        return new BaseVertexProperty(this, key, value);
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... propertyKeys) {
        checkRemoved();
        if(propertyKeys != null && propertyKeys.length > 0) throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        return this.property(key, value);
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        return edges(direction, edgeLabels, new Predicates());
    }

    public Iterator<Edge> edges(Direction direction, String[] edgeLabels, Predicates predicates) {
        List<Edge> edges = queriedEdges.get(new EdgeQueryInfo(direction, edgeLabels, predicates));
        if (edges != null) {
            return edges.iterator();
        }
        return graph.getQueryHandler().edges(this, direction, edgeLabels, predicates);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        return vertices(direction, edgeLabels, new Predicates());
    }

    public Iterator<Vertex> vertices(Direction direction, String[] edgeLabels, Predicates predicates) {
        checkRemoved();
        Iterator<Edge> edgeIterator = edges(direction, edgeLabels, predicates);
        ArrayList<Vertex> vertices = new ArrayList<>();
        if (edgeIterator != null) {
            edgeIterator.forEachRemaining(edge ->
                    vertices.add(vertexToVertex(this, edge, direction)));
        }
        return vertices.iterator();
    }

    public void setSiblings(List<Vertex> siblings) {
        this.siblings = siblings;
    }

    public void addQueriedEdges(List<Edge> edges, Direction direction, String[] edgeLabels, Predicates predicates) {
        EdgeQueryInfo queryInfo = new EdgeQueryInfo(direction, edgeLabels, predicates);
        queriedEdges.put(queryInfo, edges);
    }

    public void removeEdge(Edge edge) {
        queriedEdges.forEach((edgeQueryInfo, edges) -> edges.remove(edge));
    }

    public void applyLazyFields(MultiGetItemResponse response) {
        setLabel(response.getType());
        response.getResponse().getSource().entrySet().forEach((field) ->
                addPropertyLocal(field.getKey(), field.getValue()));
    }

    public static Vertex vertexToVertex(Vertex originalVertex, Edge edge, Direction direction) {
        switch (direction) {
            case OUT:
                return edge.inVertex();
            case IN:
                return edge.outVertex();
            case BOTH:
                Vertex outV = edge.outVertex();
                Vertex inV = edge.inVertex();
                if(outV.id().equals(inV.id()))
                    return originalVertex; //points to self
                if(originalVertex.id().equals(inV.id()))
                    return outV;
                if(originalVertex.id().equals(outV.id()))
                    return inV;
            default:
                throw new IllegalArgumentException(direction.toString());
        }
    }

    public static void setVertexSiblings(List<Vertex> siblings) {
        for (Vertex vertex : siblings) {
            if (!BaseVertex.class.isAssignableFrom(vertex.getClass())) {
                return;
            }
            ((BaseVertex)vertex).setSiblings(siblings);
        }
    }

    @Override
    public <V> VertexProperty<V> property(String key, V value) {
        checkRemoved();
        ElementHelper.validateProperty(key, value);
        BaseVertexProperty vertexProperty = (BaseVertexProperty) addPropertyLocal(key, value);
        innerAddProperty(vertexProperty);
        return vertexProperty;
    }

    protected abstract void innerAddProperty(BaseVertexProperty vertexProperty);

    @Override
    public <V> VertexProperty<V> property(final String key) {
        checkRemoved();
        if (this.properties.containsKey(key)) {
            return (VertexProperty<V>) this.properties.get(key);
        }
        else return VertexProperty.<V>empty();
    }

    @Override
    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        if (null == vertex) throw Graph.Exceptions.argumentCanNotBeNull("vertexdoc");
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        checkRemoved();
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);

        return graph.getQueryHandler().addEdge(idValue, label, this, vertex, keyValues);
    }

    @Override
    public void remove() {
        super.remove();
        edges(Direction.BOTH).forEachRemaining(edge -> edge.remove());
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        checkRemoved();
        return innerPropertyIterator(propertyKeys);
    }

    @Override
    protected void checkRemoved() {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id);
    }

    public List<Vertex> getSiblings() {
        return siblings;
    }

    private static class EdgeQueryInfo {
        private Direction direction;
        private String[] edgeLabels;
        private Predicates predicates;

        public EdgeQueryInfo(Direction direction, String[] edgeLabels, Predicates predicates) {
            this.direction = direction;
            this.edgeLabels = edgeLabels;
            this.predicates = predicates;
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

            if (direction != that.direction) return false;
            if (!Arrays.equals(edgeLabels, that.edgeLabels)) return false;
            if (predicates == null) return that.predicates == null;

            return predicates.equals(that.predicates);
        }

        @Override
        public int hashCode() {
            int result = direction != null ? direction.hashCode() : 0;
            result = 31 * result + (edgeLabels != null ? Arrays.hashCode(edgeLabels) : 0);
            result = 31 * result + (predicates != null ? predicates.hashCode() : 0);
            return result;
        }


        // endregion
    }
}
