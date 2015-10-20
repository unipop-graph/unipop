package org.unipop.elastic.basic;

import org.apache.commons.configuration.Configuration;
import org.elasticsearch.client.Client;
import org.unipop.controller.EdgeController;
import org.unipop.controller.VertexController;
import org.unipop.controllerprovider.BasicControllerManager;
import org.unipop.elastic.controller.edge.ElasticEdgeController;
import org.unipop.elastic.controller.vertex.ElasticVertexController;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.UniGraph;

import java.io.IOException;

public class BasicElasticControllerManager extends BasicControllerManager {

    private EdgeController edgeController;
    private VertexController vertexController;
    private Client client;
    private ElasticMutations elasticMutations;
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
        elasticMutations = new ElasticMutations(bulk, client, timing);
        edgeController = new ElasticEdgeController(graph, client, elasticMutations, indexName, scrollSize, refresh, timing);
        vertexController = new ElasticVertexController(graph, client, elasticMutations, indexName, scrollSize, refresh, timing);
    }

    @Override
    protected VertexController getDefaultVertexController() {
        return vertexController;
    }

    @Override
    protected EdgeController getDefaultEdgeController() {
        return edgeController;
    }

    @Override
    public void commit() { elasticMutations.commit(); }

    @Override
    public void close() {
        client.close();
        timing.print();
    }
}
