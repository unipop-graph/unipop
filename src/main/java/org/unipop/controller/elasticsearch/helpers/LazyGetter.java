package org.unipop.controller.elasticsearch.helpers;

import org.unipop.structure.BaseVertex;
import org.elasticsearch.action.get.*;
import org.elasticsearch.client.Client;

import java.util.*;

public class LazyGetter {

    private static final int MAX_LAZY_GET = 1000;
    private Client client;
    private TimingAccessor timing;
    private boolean refresh;
    private boolean executed = false;
    private HashMap<GetKey, List<BaseVertex>> keyToVertices = new HashMap();

    public LazyGetter(Client client, TimingAccessor timing, boolean refresh) {
        this.client = client;
        this.timing = timing;
        this.refresh = refresh;
    }

    public Boolean canRegister() {
        return !executed && keyToVertices.keySet().size() < MAX_LAZY_GET;
    }

    public void register(BaseVertex v, String label, String indexName) {
        GetKey key = new GetKey(v.id(), label, indexName);

        List<BaseVertex> vertices = keyToVertices.get(key);
        if (vertices == null) {
            vertices = new ArrayList();
            keyToVertices.put(key, vertices);
        }
        vertices.add(v);
    }

    public void execute() {
        if (executed) return;

        timing.start("lazyMultiGet");
        MultiGetRequestBuilder multiGetRequestBuilder = client.prepareMultiGet().setRefresh(refresh);
        keyToVertices.keySet().forEach(key -> multiGetRequestBuilder.add(key.indexName, key.type, key.id));
        MultiGetResponse multiGetItemResponses = multiGetRequestBuilder.execute().actionGet();
        timing.stop("lazyMultiGet");

        multiGetItemResponses.forEach(response -> {
            if (response.isFailed() || !response.getResponse().isExists()) {
                System.out.println(response.getFailure().getMessage());
                return;
            }
            List<BaseVertex> vertices = keyToVertices.get(new GetKey(response.getId(), response.getType(), response.getIndex()));
            if (vertices == null) return;
            vertices.forEach(vertex -> vertex.applyLazyFields(response));
        });

        executed = true;
        keyToVertices = null;
        client = null;
    }

    private class GetKey {
        private final String id;
        private final String type;
        private final String indexName;

        public GetKey(Object id, String type, String indexName) {

            this.id = id.toString();
            this.type = type;
            this.indexName = indexName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GetKey getKey = (GetKey) o;

            if (!id.equals(getKey.id)) return false;
            if (type != null && getKey.type != null && !type.equals(getKey.type)) return false;
            return indexName.equals(getKey.indexName);
        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + indexName.hashCode();
            return result;
        }
    }
}
