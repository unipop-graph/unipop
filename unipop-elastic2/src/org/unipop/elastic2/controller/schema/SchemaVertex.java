package org.unipop.elastic2.controller.schema;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.unipop.elastic2.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.elastic2.controller.schema.helpers.schemaProviders.GraphVertexSchema;
import org.unipop.elastic2.helpers.ElasticMutations;
import org.unipop.elastic2.helpers.LazyGetter;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.BaseVertexProperty;
import org.unipop.structure.UniGraph;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.StreamSupport;

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
            String writeIndex = StreamSupport.stream(schema.get().getIndices().spliterator(), false).findFirst().get();
            elasticMutations.updateElement(this, writeIndex, null, false);
        }
        catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void innerRemoveProperty(Property property) {
        String writeIndex = StreamSupport.stream(schema.get().getIndices().spliterator(), false).findFirst().get();
        elasticMutations.addElement(this, writeIndex, null, false);
    }

    @Override
    protected void innerRemove() {
        String writeIndex = StreamSupport.stream(schema.get().getIndices().spliterator(), false).findFirst().get();
        elasticMutations.deleteElement(this, writeIndex, null);
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        VertexProperty<V> property = super.property(key);
        if (property == VertexProperty.empty() && lazyGetter != null) {
            lazyGetter.execute();
            property = super.property(key);
        }
        return property;
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        ArrayList properties = new ArrayList();
        for (int i = 0 ; i < propertyKeys.length; i++) {
            VertexProperty<V> property = this.property(propertyKeys[i]);
            if (property != VertexProperty.empty()) {
                properties.add(property);
            }
        }

        if (propertyKeys.length == 0 && lazyGetter != null) {
            lazyGetter.execute();
            return super.properties(propertyKeys);
        }

        return properties.iterator();
    }
    //endregion

    //region Fields
    protected Optional<GraphVertexSchema> schema;
    protected ElasticMutations elasticMutations;
    protected LazyGetter lazyGetter;
    //endregion
}
