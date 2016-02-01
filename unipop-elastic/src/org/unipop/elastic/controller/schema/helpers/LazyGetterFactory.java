package org.unipop.elastic.controller.schema.helpers;

import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphVertexSchema;
import org.elasticsearch.client.Client;
import org.unipop.elastic.helpers.ElasticLazyGetter;
import org.unipop.elastic.helpers.TimingAccessor;

import java.util.HashMap;
import java.util.Optional;

/**
 * Created by Gilad on 18/10/2015.
 */
public class LazyGetterFactory {
    private final Client client;
    private final TimingAccessor timingAccessor;
    private HashMap<String, ElasticLazyGetter> lazyGetterHashMap = new HashMap<>();
    private GraphElementSchemaProvider schemaProvider;

    public LazyGetterFactory(Client client, GraphElementSchemaProvider schemaProvider) {
        this.client = client;
        this.timingAccessor = new TimingAccessor();
        this.schemaProvider = schemaProvider;
    }

    public ElasticLazyGetter getLazyGetter(String label) {
        ElasticLazyGetter elasticLazyGetter = lazyGetterHashMap.get(label);

        Optional<GraphVertexSchema> vertexSchema = schemaProvider.getVertexSchema(label);

        if (elasticLazyGetter == null || !elasticLazyGetter.canRegister()) {
            elasticLazyGetter = new ElasticLazyGetter(client, timingAccessor);
            lazyGetterHashMap.put(label, elasticLazyGetter);
        }
        return elasticLazyGetter;
    }
}
