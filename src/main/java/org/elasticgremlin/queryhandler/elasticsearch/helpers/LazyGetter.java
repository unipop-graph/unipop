package org.elasticgremlin.queryhandler.elasticsearch.helpers;

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
    private HashMap<String, List<BaseVertex>> idToVertices = new HashMap();
    private List<Vertex> vertices = new ArrayList<>();

    public LazyGetter(Client client) {
        this.client = client;
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

        this.vertices.add(v);
        v.setSiblings(this.vertices);
    }

    public void execute() {
        if (executed) return;

        MultiGetResponse multiGetItemResponses = client.multiGet(multiGetRequest).actionGet();
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
