package org.unipop.elastic.controller.schema;

import com.google.common.collect.FluentIterable;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphVertexSchema;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.LazyGetter;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.BaseVertexProperty;
import org.unipop.structure.UniGraph;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * Created by Roman on 9/21/2015.
 */
public class SchemaVertex<TController extends SchemaVertexController> extends BaseVertex<TController> {
    //region Constructor
    public SchemaVertex(
            Object id,
            String label,
            UniGraph graph,
            Map<String, Object> keyValues,
            TController vertexController,
            LazyGetter lazyGetter,
            GraphElementSchemaProvider schemaProvider,
            ElasticMutations elasticMutations) {
        super(id, label, keyValues, vertexController, graph);
        this.schema = schemaProvider.getVertexSchema(this.label());

        this.elasticMutations = elasticMutations;
        if (lazyGetter != null) {
            this.lazyGetter = lazyGetter;
            lazyGetter.register(this, label, this.schema.get().getIndices().iterator().next());
        }
    }
    //endregion

    //region BaseVertex Implementation
    @Override
    protected void innerAddProperty(BaseVertexProperty vertexProperty) {
        try {
            String writeIndex = FluentIterable.from(schema.get().getIndices()).first().get();
            elasticMutations.updateElement(this, writeIndex, null, false);
        }
        catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void innerRemoveProperty(Property property) {

    }

    @Override
    protected void innerRemove() {
        String writeIndex = FluentIterable.from(schema.get().getIndices()).first().get();
        elasticMutations.deleteElement(this, writeIndex, null);
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        if (lazyGetter != null) lazyGetter.execute();
        return super.property(key);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        if (lazyGetter != null) lazyGetter.execute();
        return super.properties(propertyKeys);
    }
    //endregion

    //region Fields
    protected Optional<GraphVertexSchema> schema;
    protected ElasticMutations elasticMutations;
    protected LazyGetter lazyGetter;
    //endregion
}
