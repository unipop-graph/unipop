package org.apache.tinkerpop.gremlin.elastic.elasticservice;

import org.apache.tinkerpop.gremlin.elastic.structure.ElasticVertex;
import org.elasticsearch.action.get.*;

import java.util.*;

public class LazyGetter {
    private static final int MAX_LAZY_GET = 1000;
    private boolean executed = false;
    private MultiGetRequest multiGetRequest = new MultiGetRequest();
    private HashMap<String, List<ElasticVertex>> lazyGetters = new HashMap();
    private ElasticService es;

    public LazyGetter(ElasticService es) {
        this.es = es;
    }

    public Boolean canRegister() {
        return !executed && multiGetRequest.getItems().size() < MAX_LAZY_GET;
    }

    public void register(ElasticVertex v) {
        IndexProvider.IndexResult indexProviderResult = es.indexProvider.getIndex(v);
        multiGetRequest.add(indexProviderResult.getIndex(), v.label(), v.id().toString()); //TODO: add routing..?

        putOrAddToList(lazyGetters, v.id().toString(),v);
    }
    protected void putOrAddToList(Map map, Object key, Object value) {
        Object list = map.get(key);
        if(list == null || !(list instanceof List)) {
            list = new ArrayList();
            map.put(key, list);
        }
        ((List)list).add(value);
    }

    public void execute() {
        if (executed) return;

        MultiGetResponse multiGetItemResponses = es.client.multiGet(multiGetRequest).actionGet();
        multiGetItemResponses.forEach(response -> {
            Map<String, Object> source = response.getResponse().getSource();
            lazyGetters.get(response.getId()).forEach(vertex ->
                source.entrySet().forEach((field) ->
                    vertex.addPropertyLocal(field.getKey(), field.getValue())));
        });

        executed = true;
        multiGetRequest = null;
        lazyGetters = null;
        es = null;
    }
}
