package com.tinkerpop.gremlin.elastic.structure;

import com.tinkerpop.gremlin.elastic.ElasticService;
import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.lang3.ArrayUtils;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;

import java.util.ArrayList;
import java.util.Iterator;

public class ElasticVertex extends ElasticElement implements Vertex, Vertex.Iterators {
    private ElasticService elasticService;

    public ElasticVertex(final Object id, final String label, Object[] keyValues, ElasticGraph graph) {
        super(id, label, graph, keyValues);
        elasticService = graph.elasticService;
    }

    @Override
    public Property addPropertyLocal(String key, Object value) {
        checkRemoved();
        if(!shouldAddProperty(key)) return Property.empty();
        ElasticVertexProperty vertexProperty = new ElasticVertexProperty(this, key, value);
        properties.put(key, vertexProperty);
        return vertexProperty;
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value, final Object... keyValues) {
        checkRemoved();
        return this.property(key, value);
    }

    @Override
    public <V> VertexProperty<V> property(String key, V value) {
        checkRemoved();
        ElementHelper.validateProperty(key, value);
        ElasticVertexProperty vertexProperty = (ElasticVertexProperty) addPropertyLocal(key, value);
        properties.put(key, vertexProperty);
        elasticService.addProperty(this, key, value);
        return vertexProperty;
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        checkRemoved();
        if (this.properties.containsKey(key)) {
            return (VertexProperty<V>) this.properties.get(key);
        } else return VertexProperty.<V>empty();
    }

    @Override
    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        if (null == vertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        checkRemoved();
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);

        try {
            IndexResponse response = elasticService.addElement(label, idValue, Type.edge, ArrayUtils.addAll(keyValues, ElasticEdge.InId, vertex.id(), ElasticEdge.OutId, this.id()));
            return new ElasticEdge(response.getId(), label, this.id(), vertex.id(), keyValues, graph);
        }
        catch(DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
        }
    }

    @Override
    public void remove() {
        checkRemoved();
        this.removed = true;
        elasticService.deleteElement(this);
        elasticService.deleteElements((Iterator) edgeIterator(Direction.BOTH));
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Vertex.Iterators iterators() {
        return this;
    }

    @Override
    public <V> Iterator<VertexProperty<V>> propertyIterator(final String... propertyKeys) {
        checkRemoved();
        return innerPropertyIterator(propertyKeys);
    }

    @Override
    public Iterator<Edge> edgeIterator(final Direction direction, final String... edgeLabels) {
        FilterBuilder filter;
        if (direction == Direction.IN) filter = getFilter(ElasticEdge.InId);
        else if (direction == Direction.OUT) filter = getFilter(ElasticEdge.OutId);
        else filter = FilterBuilders.orFilter(getFilter(ElasticEdge.InId), getFilter(ElasticEdge.OutId));

        return elasticService.searchEdges(filter, edgeLabels);
    }

    private FilterBuilder getFilter(String key) {
        return FilterBuilders.termFilter(key, this.id());
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction, final String... edgeLabels) {
        checkRemoved();
        Iterator<Edge> edgeIterator = edgeIterator(direction, edgeLabels);
        ArrayList<Object> ids = new ArrayList<>();
        edgeIterator.forEachRemaining((edge) -> ((ElasticEdge) edge).getVertexId(direction.opposite()).forEach((id)->ids.add(id)));
        return elasticService.getVertices(ids.toArray());
    }

    @Override
    protected void checkRemoved() {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id);
    }
}
