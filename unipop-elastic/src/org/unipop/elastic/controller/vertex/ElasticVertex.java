package org.unipop.elastic.controller.vertex;

import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.ElasticLazyGetter;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.structure.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class ElasticVertex<T extends ElasticVertexController> extends BaseVertex<T> {
    protected final ElasticMutations elasticMutations;
    protected final String indexName;
    private ElasticLazyGetter elasticLazyGetter;

    public ElasticVertex(final Object id, final String label, Map<String, Object> keyValues, T controller, UniGraph graph, ElasticLazyGetter elasticLazyGetter, ElasticMutations elasticMutations, String indexName) {
        super(id, label, keyValues, controller, graph);
        this.elasticMutations = elasticMutations;
        this.indexName = indexName;
        if (elasticLazyGetter != null) {
            this.elasticLazyGetter = elasticLazyGetter;
            elasticLazyGetter.register(this, label, this.indexName);
        }
    }

    @Override
    public String label() {
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
        checkLazy();
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
        checkLazy();
        return super.properties(propertyKeys);
    }

    protected void checkLazy() {
        if (elasticLazyGetter != null) elasticLazyGetter.execute();
    }
}
