package org.unipop.elastic.controller.vertex;

import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.LazyGetter;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.structure.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class ElasticVertex extends BaseVertex<ElasticVertexController> {
    protected final ElasticMutations elasticMutations;
    protected final String indexName;
    private LazyGetter lazyGetter;

    public ElasticVertex(final Object id, final String label, Map<String, Object> keyValues, ElasticVertexController controller, UniGraph graph, LazyGetter lazyGetter, ElasticMutations elasticMutations, String indexName) {
        super(id, label, keyValues, controller, graph);
        this.elasticMutations = elasticMutations;
        this.indexName = indexName;
        if (lazyGetter != null) {
            this.lazyGetter = lazyGetter;
            lazyGetter.register(this, label, this.indexName);
        }
    }

    @Override
    public String label() {
        if (this.label == null) checkLazy();
        return super.label();
    }

    @Override
    protected void innerAddProperty(BaseVertexProperty vertexProperty) {
        try {
            elasticMutations.updateElement(this, indexName, null, false);
        }
        catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        if(properties.size() == 0) checkLazy();
        return super.property(key);
    }

    @Override
    protected void innerRemoveProperty(Property property) {
        elasticMutations.addElement(this, indexName, null, false);
    }

    @Override
    protected void innerRemove() {
        elasticMutations.deleteElement(this, indexName, null);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        if(properties.size() == 0) checkLazy();
        return super.properties(propertyKeys);
    }

    private void checkLazy() {
        if (lazyGetter != null) lazyGetter.execute();
    }
}
