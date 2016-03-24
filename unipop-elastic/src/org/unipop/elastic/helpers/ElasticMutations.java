package org.unipop.elastic.helpers;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.unipop.elastic.schema.ElementSchema;
import org.unipop.structure.BaseElement;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

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

    public void addElement(BaseElement element, String index, String routing,  boolean create) {
        IndexRequestBuilder indexRequest = client.prepareIndex(index, element.label(), element.id().toString())
                .setSource(element.allFields()).setRouting(routing).setCreate(create);

        isDirty = true;
        indicesToRefresh.add(index);

        if(bulkRequest != null) bulkRequest.add(indexRequest);
        else indexRequest.execute().actionGet();
        revision++;
    }


    public void updateElement(Element element, boolean upsert)  {
        ElementSchema schema = element.<ElementSchema>value("~schema");
        String index = element.<String>value("~index");
        UpdateRequest updateRequest = new UpdateRequest(index, element.value("~type"), element.value("~id"))
                .doc(schema.getFields(element)).routing(element.<String>property("~routing").orElse(null));

        isDirty = true;
        indicesToRefresh.add(index);

        if(upsert)
            updateRequest.detectNoop(true).docAsUpsert(true);
        if(bulkRequest != null) bulkRequest.add(updateRequest);
        else client.update(updateRequest).actionGet();
        revision++;
    }


    public void deleteElement(Element element) {
        String index = element.<String>value("~index");
        DeleteRequestBuilder deleteRequestBuilder = client
                .prepareDelete(index, element.value("~type"), element.value("~id"))
                .setRouting(element.<String>property("~routing").orElse(null));

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
