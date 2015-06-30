package org.elasticgremlin.elasticsearch;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticgremlin.structure.BaseVertex;
import org.elasticsearch.action.get.*;
import org.elasticsearch.client.Client;

import java.util.*;

public class LazyGetter {

    private static final int MAX_LAZY_GET = 1000;
    private Client client;
    private boolean executed = false;
    private MultiGetRequest multiGetRequest = new MultiGetRequest();
    private HashMap<String, List<BaseVertex>> lazyVertices = new HashMap();
    private List<Vertex> siblings = new ArrayList<>();

    public LazyGetter(Client client) {
        this.client = client;
    }

    public Boolean canRegister() {
        return !executed && multiGetRequest.getItems().size() < MAX_LAZY_GET;
    }

    public void register(BaseVertex v, String indexName) {
        multiGetRequest.add(indexName, null, v.id().toString()); //TODO: add routing..?
        putOrAddToList(lazyVertices, v.id().toString(), v);
        addSiblings(v);
    }

    protected void addSiblings(BaseVertex v) {
        siblings.add(v);
        v.setSiblings(siblings);
    }

    protected void putOrAddToList(Map map, Object key, Object value) {
        Object list = map.get(key);
        if (list == null || !(list instanceof List)) {
            list = new ArrayList();
            map.put(key, list);
        }
        ((List) list).add(value);
    }

    public void execute() {
        if (executed) return;

        MultiGetResponse multiGetItemResponses = client.multiGet(multiGetRequest).actionGet();
        multiGetItemResponses.forEach(response -> {
            GetResponse getResponse = response.getResponse();
            if (getResponse == null || !getResponse.isExists()) return;
            List<BaseVertex> vertices = lazyVertices.get(response.getId());
            if (vertices == null) return;
            vertices.forEach(vertex -> vertex.applyLazyFields(response));
        });

        executed = true;
        multiGetRequest = null;
        lazyVertices = null;
        client = null;
    }
}
