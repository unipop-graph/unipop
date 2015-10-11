package org.unipop.elastic.custom;

import org.apache.commons.configuration.Configuration;
import org.elasticsearch.client.Client;
import org.unipop.controllerprovider.GraphSchemaControllerProvider;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.UniGraph;

import java.io.IOException;

public class ModernGraphControllerProvider extends GraphSchemaControllerProvider {
    private Client client;
    private TimingAccessor timing;

    @Override
    public void init(UniGraph graph, Configuration configuration) throws IOException {
        String indexName = configuration.getString("elasticsearch.index.name", "graph");
        boolean refresh = configuration.getBoolean("elasticsearch.refresh", true);
        int scrollSize = configuration.getInt("elasticsearch.scrollSize", 500);
        boolean bulk = configuration.getBoolean("elasticsearch.bulk", false);

        client = ElasticClientFactory.create(configuration);
        ElasticHelper.createIndex(indexName, client);

        timing = new TimingAccessor();
        //elasticMutations = new ElasticMutations(bulk, client, timing);
        //docEdgeHandler = new ElasticEdgeController(graph, client, elasticMutations, indexName, scrollSize, refresh, timing);
        //elasticDocVertexHandler = new ElasticVertexController(graph, client, elasticMutations, indexName, scrollSize, refresh, timing);

       // this.schema.addVertex(T.label, "person", U.controller, )

    }

    @Override
    public void commit() {

    }

    @Override
    public void printStats() {

    }

    @Override
    public void close() {

    }
}
