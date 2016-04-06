package org.unipop.elastic.controller.helpers;

import org.elasticsearch.action.get.*;
import org.elasticsearch.client.Client;
import org.unipop.structure.BaseVertex;

import java.util.*;

public class ElasticLazyGetter implements LazzyGetter{

    private static final int MAX_LAZY_GET = 50;
    private Client client;
    private TimingAccessor timing;
    private boolean executed = false;
    private HashMap<GetKey, List<BaseVertex>> keyToVertices = new HashMap();

    public ElasticLazyGetter(Client client, TimingAccessor timing) {
        this.client = client;
        this.timing = timing;
    }

    @Override
    public Boolean canRegister() {
        return !executed && keyToVertices.keySet().size() < MAX_LAZY_GET;
    }

    @Override
    public void register(BaseVertex v, String label, String indexName) {
        if(executed) System.out.println("This ElasticLazyGetter has already been executed.");

        GetKey key = new GetKey(v.id(), label, indexName);

        List<BaseVertex> vertices = keyToVertices.get(key);
        if (vertices == null) {
            vertices = new ArrayList();
            keyToVertices.put(key, vertices);
        }
        vertices.add(v);
    }

    @Override
    public void execute() {
        if (executed) return;
        executed = true;

        timing.start("lazyMultiGet");

        MultiGetRequestBuilder multiGetRequestBuilder = client.prepareMultiGet();
        keyToVertices.keySet().forEach(key-> multiGetRequestBuilder.add(key.indexName, key.type, key.id));

        MultiGetResponse response = multiGetRequestBuilder.execute().actionGet();
        response.forEach(hit -> {
            List<BaseVertex> baseVertices = keyToVertices.get(new GetKey(hit.getId(), hit.getType(), hit.getIndex()));
            Map<String, Object> hitSource = hit.getResponse().getSource();
            baseVertices.forEach(baseVertex -> baseVertex.applyLazyFields(hit.getType(), hitSource));
        });
        timing.stop("lazyMultiGet");

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
            return true;
        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
//            result = 31 * result + indexName.hashCode();
            return result;
        }
    }
}
