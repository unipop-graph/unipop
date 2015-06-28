package org.elasticgremlin.elasticsearch;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticgremlin.structure.BaseElement;
import org.elasticsearch.action.get.*;
import org.elasticsearch.client.Client;

import java.util.*;
import java.util.function.Consumer;

public class LazyGetter {

    public interface LazyConsumer extends Element {
        void applyLazyFields(MultiGetItemResponse response);
    }

    private static final int MAX_LAZY_GET = 1000;
    private Client client;
    private boolean executed = false;
    private MultiGetRequest multiGetRequest = new MultiGetRequest();
    private HashMap<String, List<LazyConsumer>> lazyGetters = new HashMap();
    private Consumer<List<Vertex>> siblingsFunc;

    public LazyGetter(Client client, Consumer<List<Vertex>> siblingsFunc) {
        this.client = client;
        this.siblingsFunc = siblingsFunc;
    }

    public Boolean canRegister() {
        return !executed && multiGetRequest.getItems().size() < MAX_LAZY_GET;
    }

    public void register(LazyConsumer v, String indexName) {
        multiGetRequest.add(indexName, null, v.id().toString()); //TODO: add routing..?
        putOrAddToList(lazyGetters, v.id().toString(), v);
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
            List<LazyConsumer> vertices = lazyGetters.get(response.getId());
            if (vertices == null) return;
            vertices.forEach(vertex -> vertex.applyLazyFields(response));
        });

        // Apply siblings
        List<Vertex> allVertices = new ArrayList<>();
        lazyGetters.forEach((id, vertices) -> {
            vertices.forEach(lazyConsumer -> {
                if (!Vertex.class.isAssignableFrom(lazyConsumer.getClass())) return;
                allVertices.add((Vertex) lazyConsumer);
            });
        });
        siblingsFunc.accept(allVertices);

        executed = true;
        multiGetRequest = null;
        lazyGetters = null;
        client = null;
    }
}
