package org.unipop.schema.reference;

import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.structure.UniVertex;
import org.unipop.structure.UniGraph;

import java.util.*;

public class DeferredVertex extends UniVertex {

    public DeferredVertex(Map<String, Object> properties, UniGraph graph) {
        super(properties, graph);
    }

    boolean deferred = true;

    public boolean isDeferred() {
        return deferred;
    }

    private void validateProperties() {
        if (deferred) {
            DeferredVertexQuery query = new DeferredVertexQuery(Collections.singletonList(this), null, null, null);
            this.graph.getControllerManager().getControllers(DeferredVertexQuery.DeferredVertexController.class).forEach(deferredController ->
                    deferredController.fetchProperties(query));
        }
    }

    public void loadProperties(Vertex vertex) {
        deferred = false;
        vertex.properties().forEachRemaining(prop -> addPropertyLocal(prop.key(), prop.value()));
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        validateProperties();
        return super.property(cardinality, key, value, keyValues);
    }

    @Override
    public  <V> VertexProperty<V> property(String key, V value) {
        validateProperties();
        return super.property(key, value);
    }

    @Override
    public  <V> VertexProperty<V> property(String key) {
        validateProperties();
        return super.property(key);
    }

    @Override
    public  <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        validateProperties();
        return super.properties(propertyKeys);
    }

    @Override
    public Set<String> keys() {
        validateProperties();
        return super.keys();
    }

    @Override
    public void removeProperty(Property property) {
        validateProperties();
        super.removeProperty(property);
    }
}
