package com.tinkerpop.gremlin.elastic.structure;

import com.tinkerpop.gremlin.elastic.elasticservice.*;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.util.*;
import org.elasticsearch.index.query.*;

import java.util.*;

public class ElasticVertex extends ElasticElement implements Vertex, Vertex.Iterators {
    private LazyGetter lazyGetter;
    private ElasticService elasticService;

    public ElasticVertex(final Object id, final String label, Object[] keyValues, ElasticGraph graph, Boolean lazy) {
        super(id, label, graph, keyValues);
        elasticService = graph.elasticService;
        if(lazy) {
            this.lazyGetter = graph.elasticService.getLazyGetter();
            lazyGetter.register(this);
        }
    }

    @Override
    public Property createProperty(String key, Object value) {
        return new ElasticVertexProperty(this, key, value);
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
        elasticService.addProperty(this, key, value);
        return vertexProperty;
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        checkRemoved();
        if(lazyGetter != null) lazyGetter.execute();
        if (this.properties.containsKey(key)) {
            return (VertexProperty<V>) this.properties.get(key);
        } else return VertexProperty.<V>empty();
    }

    @Override
    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        if (null == vertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        checkRemoved();
        return graph.addEdge(label, this.id(), this.label(), vertex.id(), vertex.label(), keyValues);
    }

    @Override
    public void remove() {
        checkRemoved();
        elasticService.deleteElement(this);
        elasticService.deleteElements((Iterator) edgeIterator(Direction.BOTH));
        this.removed = true;
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
        if(lazyGetter != null) lazyGetter.execute();
        return innerPropertyIterator(propertyKeys);
    }

    @Override
    public Iterator<Edge> edgeIterator(final Direction direction, final String... edgeLabels) {
        ArrayList<HasContainer> hasList = new ArrayList<>();

        if(edgeLabels != null && edgeLabels.length > 0)
            hasList.add(new HasContainer(T.label, Contains.within, edgeLabels));

        return elasticService.searchEdges(hasList, null, direction, this.id());
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction, final String... edgeLabels) {
        checkRemoved();
        Iterator<Edge> edgeIterator = edgeIterator(direction, edgeLabels);
        ArrayList<Object> ids = new ArrayList<>();
        edgeIterator.forEachRemaining((edge) -> ((ElasticEdge) edge).getVertexId(direction.opposite()).forEach((id) -> ids.add(id)));
        return elasticService.getVertices(null, null, ids.toArray());
    }

    @Override
    protected void checkRemoved() {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id);
    }
}
