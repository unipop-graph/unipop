package org.elasticgremlin.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.*;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.elasticgremlin.queryhandler.Predicates;
import org.elasticgremlin.queryhandler.elasticsearch.helpers.ElasticMutations;
import org.elasticsearch.action.get.MultiGetItemResponse;

import java.util.*;

public abstract class BaseVertex extends BaseElement implements Vertex {

    private final ElasticMutations elasticMutations;
    private HashMap<EdgeQueryInfo, List<Edge>> queriedEdges = new HashMap<>();
    protected List<BaseVertex> siblings;

    protected BaseVertex(Object id, String label, ElasticGraph graph, Object[] keyValues, ElasticMutations elasticMutations) {
        super(id, label, graph, keyValues);
        this.elasticMutations = elasticMutations;
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

    public void setSiblings(List<BaseVertex> siblings) {
        this.siblings = siblings;
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
        if (null == vertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        checkRemoved();
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);

        return graph.getQueryHandler().addEdge(idValue, label, this, vertex, keyValues);
    }

    @Override
    public void remove() {
        super.remove();
        edges(Direction.BOTH).forEachRemaining(Element::remove);
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

    public Iterator<Edge> edges(Direction direction, String[] edgeLabels, Predicates predicates) {
        EdgeQueryInfo queryInfo = new EdgeQueryInfo(direction, edgeLabels, predicates, elasticMutations.getRevision());
        List<Edge> edges = queriedEdges.get(queryInfo);
        if (edges != null)  return edges.iterator();

        Iterator<BaseVertex> vertices = siblings == null ? IteratorUtils.asIterator(this) : siblings.iterator();

        Map<Object, List<Edge>> vertexToEdge = graph.getQueryHandler().edges(vertices, direction, edgeLabels, predicates);
        siblings.forEach(sibling -> sibling.addQueriedEdges(queryInfo, vertexToEdge.get(sibling.id())));

        List<Edge> thisEdges = vertexToEdge.get(this.id());
        return thisEdges != null ? thisEdges.iterator() : Collections.emptyIterator();
    }

    private void addQueriedEdges(EdgeQueryInfo queryInfo, List<Edge> edges) {
        queriedEdges.put(queryInfo, edges);
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
            return predicates.equals(that.predicates);

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
}
