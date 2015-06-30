package org.elasticgremlin.elasticsearch.vertex;

import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.elasticsearch.ElasticMutations;
import org.elasticgremlin.structure.*;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;

public class ElasticDocVertex extends BaseVertex {
    private final ElasticMutations elasticMutations;
    private final String indexName;
    private LazyGetter lazyGetter;

    public ElasticDocVertex(final Object id, final String label, Object[] keyValues, ElasticGraph graph, LazyGetter lazyGetter, ElasticMutations elasticMutations, String indexName) {
        super(id, label, graph, keyValues);
        this.elasticMutations = elasticMutations;
        this.indexName = indexName;
        if(lazyGetter != null) {
            this.lazyGetter = lazyGetter;
            lazyGetter.register(this);
        }
    }

    @Override
    public String label() {
        if(this.label == null && lazyGetter != null) lazyGetter.execute();
        return super.label();
    }

    @Override
    protected void innerAddProperty(BaseVertexProperty vertexProperty) {
        try {
            elasticMutations.updateElement(this, indexName, null, false);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        if(lazyGetter != null) lazyGetter.execute();
        return super.property(key);
    }

    @Override
    protected void innerRemoveProperty(Property property) {
        try {
            elasticMutations.updateElement(this, indexName, null, false);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void innerRemove() {
        elasticMutations.deleteElement(this, indexName, null);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        if(lazyGetter != null) lazyGetter.execute();
        return super.properties(propertyKeys);
    }
}
