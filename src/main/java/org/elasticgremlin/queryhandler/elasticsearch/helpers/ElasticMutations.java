package org.elasticgremlin.queryhandler.elasticsearch.helpers;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.elasticgremlin.structure.BaseElement;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class ElasticMutations {

    private Client client;
    private BulkRequestBuilder bulkRequest;
    private TimingAccessor timing;

    public ElasticMutations(Boolean bulk, Client client, TimingAccessor timing) {
        if(bulk) bulkRequest = client.prepareBulk();
        this.timing = timing;
        this.client = client;
    }

    public void addElement(Element element, String index, String routing,  boolean create) {
        IndexRequestBuilder indexRequest = client.prepareIndex(index, element.label(), element.id().toString())
                .setSource(propertiesMap(element)).setRouting(routing).setCreate(create);
        if(bulkRequest != null) {
            bulkRequest.add(indexRequest);
        }
        else {
            timing.start("index");
            indexRequest.execute().actionGet();
            timing.stop("index");
        }
    }

    public void updateElement(Element element, String index, String routing, boolean upsert) throws ExecutionException, InterruptedException {
        UpdateRequest updateRequest = new UpdateRequest(index, element.label(), element.id().toString())
                .doc(propertiesMap(element)).routing(routing);
        if(upsert)
            updateRequest.detectNoop(true).docAsUpsert(true);
        if(bulkRequest != null) {
            bulkRequest.add(updateRequest);
        }
        else {
            timing.start("update");
            client.update(updateRequest).actionGet();
            timing.stop("update");
        }
    }


    public void deleteElement(Element element, String index, String routing) {
        DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete(index, element.label(), element.id().toString()).setRouting(routing);
        if(bulkRequest != null) {
            bulkRequest.add(deleteRequestBuilder);
        }
        else {
            timing.start("delete");
            deleteRequestBuilder.execute().actionGet();
            timing.stop("delete");
        }
    }

    public void commit() {
        if(bulkRequest == null) return;
        timing.start("bulk");
        bulkRequest.execute().actionGet();
        timing.stop("bulk");

        bulkRequest = client.prepareBulk();
    }

    private Map propertiesMap(Element element) {
        if(element instanceof BaseElement)
            return ((BaseElement)element).allFields();

        Map<String, Object> map = new HashMap<>();
        element.properties().forEachRemaining(property -> map.put(property.key(), property.value()));
        return map;
    }
}
