package org.apache.tinkerpop.gremlin.elastic.elasticservice;

import org.apache.tinkerpop.gremlin.elastic.structure.ElasticVertex;
import org.elasticsearch.action.get.*;

import java.util.*;

public class LazyGetter {
    private static final int MAX_LAZY_GET = 1000;
    private boolean executed = false;
    private MultiGetRequest multiGetRequest = new MultiGetRequest();
    private HashMap<String, ElasticVertex> lazyGetters = new HashMap();
    private ElasticService es;

    public LazyGetter(ElasticService es) {
        this.es = es;
    }

    public Boolean canRegister() {
        return !executed && multiGetRequest.getItems().size() < MAX_LAZY_GET;
    }

    public void register(ElasticVertex v) {
        SchemaProvider.Result schemaProviderResult = es.schemaProvider.getIndex(v.label(), v.id(), ElasticService.ElementType.vertex, null);
        multiGetRequest.add(schemaProviderResult.getIndex(), v.label(), v.id().toString()); //TODO: add routing..?
        lazyGetters.put(v.id().toString(), v);
    }

    public void execute() {
        if (executed) return;

        MultiGetResponse multiGetItemResponses = es.client.multiGet(multiGetRequest).actionGet();
        multiGetItemResponses.forEach(response -> {
            Map<String, Object> source = response.getResponse().getSource();
            ElasticVertex v = lazyGetters.get(response.getId());
            source.entrySet().forEach((field) -> v.addPropertyLocal(field.getKey(), field.getValue()));
        });

        executed = true;
        multiGetRequest = null;
        lazyGetters = null;
        es = null;
    }
}
