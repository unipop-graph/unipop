package org.elasticgremlin.queryhandler.elasticsearch.helpers;

import org.elasticgremlin.structure.BaseVertex;
import org.elasticsearch.action.get.*;
import org.elasticsearch.client.Client;

import java.util.*;

public class LazyGetter {

    private static final int MAX_LAZY_GET = 1000;
    private Client client;
    private TimingAccessor timing;
    private boolean executed = false;
    private MultiGetRequest multiGetRequest = new MultiGetRequest();
    private HashMap<String, List<BaseVertex>> idToVertices = new HashMap();

    public LazyGetter(Client client, TimingAccessor timing) {
        this.client = client;
        this.timing = timing;
    }

    public Boolean canRegister() {
        return !executed && multiGetRequest.getItems().size() < MAX_LAZY_GET;
    }

    public void register(BaseVertex v, String indexName) {
        multiGetRequest.add(indexName, null, v.id().toString()); //TODO: add routing..?

        List<BaseVertex> vertices = idToVertices.get(v.id().toString());
        if (vertices == null) {
            vertices = new ArrayList();
            idToVertices.put(v.id().toString(), vertices);
        }
        vertices.add(v);

    }

    public void execute() {
        if (executed) return;

        timing.start("lazyMultiGet");
        MultiGetResponse multiGetItemResponses = client.multiGet(multiGetRequest).actionGet();
        timing.stop("lazyMultiGet");

        multiGetItemResponses.forEach(response -> {
            if (response.isFailed() || !response.getResponse().isExists()) {
                System.out.println(response.getFailure().getMessage());
                return;
            }
            List<BaseVertex> vertices = idToVertices.get(response.getId());
            if (vertices == null) return;
            vertices.forEach(vertex -> vertex.applyLazyFields(response));
        });

        executed = true;
        multiGetRequest = null;
        idToVertices = null;
        client = null;
    }
}
