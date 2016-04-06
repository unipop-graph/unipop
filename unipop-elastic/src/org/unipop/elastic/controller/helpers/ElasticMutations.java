package org.unipop.elastic.controller.helpers;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.unipop.elastic.schema.ElasticElementSchema;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ElasticMutations {

    private final TimingAccessor timing;
    private Client client;
    private BulkRequestBuilder bulkRequest;
    private int revision = 0;
    private boolean isDirty = false;
    private Set<String> indicesToRefresh = new HashSet<>();

    public ElasticMutations(Boolean bulk, Client client, TimingAccessor timing) {
        if(bulk) bulkRequest = client.prepareBulk();
        this.timing = timing;
        this.client = client;
    }

    public void refreshIfDirty() {
        if (isDirty) {
            indicesToRefresh.forEach(
                    s -> client.admin().indices().refresh(new RefreshRequest(s)).actionGet()
            );
        }
        isDirty = false;
        indicesToRefresh.clear();
    }

    public <E extends Element> void addElement(E element, boolean create) {
        ElasticElementSchema<E> schema = element.<ElasticElementSchema<E>>value("~schema");
        String index = element.<String>value("~index");
        String type = element.value("~type");
        String id = element.value("~id").toString();
        String routing = element.<String>property("~routing").orElse(null);
        Map<String, Object> fields = schema.toFields(element);
        IndexRequestBuilder indexRequest = client.prepareIndex(index, type, id)
                .setSource(fields).setRouting(routing).setCreate(create);

        isDirty = true;
        indicesToRefresh.add(index);

        if(bulkRequest != null) bulkRequest.add(indexRequest);
        else indexRequest.execute().actionGet();
        revision++;
    }


    public <E extends Element> void updateElement(E element, boolean upsert)  {
        ElasticElementSchema<E> schema = element.<ElasticElementSchema<E>>value("~schema");
        String index = element.<String>value("~index");
        String type = element.value("~type");
        String id = element.value("~id").toString();
        String routing = element.<String>property("~routing").orElse(null);
        Map<String, Object> fields = schema.toFields(element);

        UpdateRequest updateRequest = new UpdateRequest(index,type, id).doc(fields).routing(routing);

        isDirty = true;
        indicesToRefresh.add(index);

        if(upsert)
            updateRequest.detectNoop(true).docAsUpsert(true);
        if(bulkRequest != null) bulkRequest.add(updateRequest);
        else client.update(updateRequest).actionGet();
        revision++;
    }


    public void deleteElement(Element element) {
        ElasticElementSchema schema = element.<ElasticElementSchema>value("~schema");
        String index = element.<String>value("~index");
        String type = element.value("~type");
        String id = element.value("~id").toString();
        String routing = element.<String>property("~routing").orElse(null);

        DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete(index, type, id).setRouting(routing);

        isDirty = true;
        indicesToRefresh.add(index);

        if(bulkRequest != null) bulkRequest.add(deleteRequestBuilder);
        else deleteRequestBuilder.execute().actionGet();
        revision++;
    }

    public void commit() {
        if (bulkRequest == null) return;
        timing.start("bulk");
        bulkRequest.execute().actionGet();
        timing.stop("bulk");
    }

    public void refresh(String... indices) {
        if (revision > 0) {
            client.admin().indices().prepareRefresh(indices).execute().actionGet();
            revision = 0;
        }
    }
}
