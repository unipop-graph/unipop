package org.elasticgremlin.elasticsearch;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.elasticgremlin.structure.BaseElement;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.*;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.index.*;
import org.elasticsearch.action.update.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class ElasticMutations {

    private boolean bulk;
    private Client client;
    private BulkRequestBuilder bulkRequest;

    public ElasticMutations(Configuration configuration, Client client) {
        this.bulk = configuration.getBoolean("elasticsearch.bulk", false);
        if(bulk) bulkRequest = client.prepareBulk();
        this.client = client;
    }

    public void addElement(Element element, String index, String routing,  boolean create) {
        IndexRequestBuilder indexRequest = client.prepareIndex(index, element.label(), element.id().toString())
                .setSource(propertiesMap(element)).setRouting(routing).setCreate(create);
        if(bulk) bulkRequest.add(indexRequest);
        else indexRequest.execute().actionGet();
    }

    private Map propertiesMap(Element element) {
        if(element instanceof BaseElement)
            return ((BaseElement)element).allFields();

        Map<String, Object> map = new HashMap<>();
        element.properties().forEachRemaining(property -> map.put(property.key(), property.value()));
        return map;
    }

    public void updateElement(Element element, String index, String routing, boolean upsert) throws ExecutionException, InterruptedException {
        UpdateRequest updateRequest = new UpdateRequest(index, element.label(), element.id().toString())
                .doc(propertiesMap(element)).routing(routing);
        if(upsert)
            updateRequest.detectNoop(true).docAsUpsert(true);
        if(bulk) bulkRequest.add(updateRequest);
        else client.update(updateRequest).actionGet();
    }


    public void deleteElement(Element element, String index, String routing) {
        DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete(index, element.label(), element.id().toString()).setRouting(routing);
        if(bulk) bulkRequest.add(deleteRequestBuilder);
        else deleteRequestBuilder.execute().actionGet();
    }

    public DeleteByQueryResponse clearAllData(String[] indices) {
        return client.prepareDeleteByQuery(indices)
                .setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
    }
}
