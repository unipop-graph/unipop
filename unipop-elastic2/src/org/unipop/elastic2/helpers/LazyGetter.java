package org.unipop.elastic2.helpers;

import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.Client;
import org.unipop.structure.BaseVertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class LazyGetter {

    private static final int MAX_LAZY_GET = 50;
    private Client client;
    private TimingAccessor timing;
    private boolean executed = false;
    private HashMap<GetKey, List<BaseVertex>> keyToVertices = new HashMap();

    public LazyGetter(Client client, TimingAccessor timing) {
        this.client = client;
        this.timing = timing;
    }

    public Boolean canRegister() {
        return !executed && keyToVertices.keySet().size() < MAX_LAZY_GET;
    }

    public void register(BaseVertex v, String label, String indexName) {
        if(executed) System.out.println("This LazyGetter has already been executed.");

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
        executed = true;

        timing.start("lazyMultiGet");
        MultiGetRequestBuilder multiGetRequestBuilder = client.prepareMultiGet();
        keyToVertices.keySet().forEach(key -> multiGetRequestBuilder.add(key.indexName, key.type, key.id));
        MultiGetResponse multiGetItemResponses = multiGetRequestBuilder.execute().actionGet();
        timing.stop("lazyMultiGet");

        multiGetItemResponses.forEach(response -> {
            if (response.isFailed()) {
                System.out.println(response.getFailure().getMessage());
                return;
            }
            if (!response.getResponse().isExists()) {
                return;
            }
            List<BaseVertex> vertices = keyToVertices.get(new GetKey(response.getId(), response.getType(), response.getIndex()));
            if (vertices == null) return;
            vertices.forEach(vertex -> vertex.applyLazyFields(response.getType(), response.getResponse().getSource()));
        });

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
