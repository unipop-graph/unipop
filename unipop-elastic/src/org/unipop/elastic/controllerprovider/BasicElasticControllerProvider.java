package org.unipop.elastic.controllerprovider;

import org.apache.commons.configuration.Configuration;
import org.elasticsearch.client.Client;
import org.unipop.controller.provider.ControllerProvider;
import org.unipop.elastic.controller.edge.ElasticEdgeController;
import org.unipop.elastic.controller.vertex.ElasticVertexController;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.UniGraph;

public class BasicElasticControllerProvider extends ControllerProvider {

    @Override
    public void init(UniGraph graph, Configuration configuration) throws Exception {
        String indexName = configuration.getString("graphName", "unipop");

        Client client = ElasticClientFactory.create(configuration);
        ElasticHelper.createIndex(indexName, client);

        TimingAccessor timing = new TimingAccessor();
        ElasticMutations elasticMutations = new ElasticMutations(false, client, timing);
        ElasticEdgeController edgeController = new ElasticEdgeController(graph, client, elasticMutations, indexName, 0, timing);
        ElasticVertexController vertexController = new ElasticVertexController(graph, client, elasticMutations, indexName, 0, timing);

        this.addController(edgeController);
        this.addController(vertexController);
    }
}
