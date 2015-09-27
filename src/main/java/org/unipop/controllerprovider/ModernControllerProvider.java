package org.unipop.controllerprovider;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.elasticsearch.client.Client;
import org.unipop.controller.elasticsearch.edge.EdgeController;
import org.unipop.controller.elasticsearch.helpers.ElasticClientFactory;
import org.unipop.controller.elasticsearch.helpers.ElasticHelper;
import org.unipop.controller.elasticsearch.helpers.ElasticMutations;
import org.unipop.controller.elasticsearch.helpers.TimingAccessor;
import org.unipop.controller.elasticsearch.vertex.VertexController;
import org.unipop.structure.UniGraph;

import java.io.IOException;

public class ModernControllerProvider extends GraphSchemaControllerProvider {
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
        //docEdgeHandler = new EdgeController(graph, client, elasticMutations, indexName, scrollSize, refresh, timing);
        //elasticDocVertexHandler = new VertexController(graph, client, elasticMutations, indexName, scrollSize, refresh, timing);

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
