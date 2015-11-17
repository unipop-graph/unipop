package org.unipop.elastic.controller.schema.helpers;

import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphVertexSchema;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.FluentIterable;
import org.unipop.elastic.helpers.LazyGetter;
import org.unipop.elastic.helpers.TimingAccessor;

import java.util.HashMap;
import java.util.Optional;

/**
 * Created by Gilad on 18/10/2015.
 */
public class LazyGetterFactory {
    private final Client client;
    private final TimingAccessor timingAccessor;
    private HashMap<String, LazyGetter> lazyGetterHashMap = new HashMap<>();
    private GraphElementSchemaProvider schemaProvider;

    public LazyGetterFactory(Client client, GraphElementSchemaProvider schemaProvider) {
        this.client = client;
        this.timingAccessor = new TimingAccessor();
        this.schemaProvider = schemaProvider;
    }

    public LazyGetter getLazyGetter(String label) {
        LazyGetter lazyGetter = lazyGetterHashMap.get(label);

        Optional<GraphVertexSchema> vertexSchema = schemaProvider.getVertexSchema(label);

        if (lazyGetter == null || !lazyGetter.canRegister()) {
            lazyGetter = new LazyGetter(client, timingAccessor);
            lazyGetterHashMap.put(label, lazyGetter);
        }
        return lazyGetter;
    }
}
