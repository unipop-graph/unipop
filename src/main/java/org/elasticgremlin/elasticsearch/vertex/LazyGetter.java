package org.elasticgremlin.elasticsearch.vertex;

import org.elasticgremlin.structure.BaseElement;
import org.elasticsearch.action.get.*;
import org.elasticsearch.client.Client;

import java.util.*;

public class LazyGetter {
    private static final int MAX_LAZY_GET = 1000;
    private Client client;
    private final String indexName;
    private boolean executed = false;
    private MultiGetRequest multiGetRequest = new MultiGetRequest();
    private HashMap<String, List<BaseElement>> lazyGetters = new HashMap();

    public LazyGetter(Client client, String indexName) {
        this.client = client;
        this.indexName = indexName;
    }

    public Boolean canRegister() {
        return !executed && multiGetRequest.getItems().size() < MAX_LAZY_GET;
    }

    public void register(ElasticDocVertex v) {
        multiGetRequest.add(indexName, null, v.id().toString()); //TODO: add routing..?
        putOrAddToList(lazyGetters, v.id().toString(), v);
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

        MultiGetResponse multiGetItemResponses = client.multiGet(multiGetRequest).actionGet();
        multiGetItemResponses.forEach(response -> {
            Map<String, Object> source = response.getResponse().getSource();
            List<BaseElement> vertices = lazyGetters.get(response.getId());
            if(vertices == null) return;
            vertices.forEach(vertex -> {
                vertex.setLabel(response.getType());
                source.entrySet().forEach((field) ->
                        vertex.addPropertyLocal(field.getKey(), field.getValue()));
            });
        });

        executed = true;
        multiGetRequest = null;
        lazyGetters = null;
        client = null;
    }
}
