package org.unipop.elastic.helpers;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.unipop.controller.Predicates;
import org.unipop.helpers.LazzyGetter;
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


        Set<String> types = new HashSet<>();
        Set<String> indices = new HashSet<>();
        Set<Object> ids = new HashSet<>();

        keyToVertices.keySet().forEach(getKey -> {
            types.add(getKey.type);
            ids.add(getKey.id);
            indices.add(getKey.indexName);
        });

        Predicates p = new Predicates();
        p.hasContainers.add(new HasContainer(T.id.getAccessor(), P.within(ids)));

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indices.toArray(new String[indices.size()]))
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), ElasticHelper.createFilterBuilder(p.hasContainers))).setSize(ids.size());

        SearchResponse response = searchRequestBuilder.execute().actionGet();
        response.getHits().forEach(hit -> {
            keyToVertices.get(new GetKey(hit.id(), hit.type(), hit.getIndex())).forEach(baseVertex ->
            {
                Map<String,Object> source = hit.getSource();

                baseVertex.applyLazyFields(hit.type(), source);
            });
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
