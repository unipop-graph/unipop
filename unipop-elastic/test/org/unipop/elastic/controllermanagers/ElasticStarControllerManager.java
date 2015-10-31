package org.unipop.elastic.controllermanagers;

import org.apache.commons.configuration.Configuration;
import org.elasticsearch.client.Client;
import org.unipop.controller.EdgeController;
import org.unipop.controller.VertexController;
import org.unipop.controllerprovider.BasicControllerManager;
import org.unipop.elastic.controller.star.StarController;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.UniGraph;

/**
 * Created by sbarzilay on 10/26/15.
 */
public class ElasticStarControllerManager extends BasicControllerManager {
    private StarController controller;
    private Client client;
    private ElasticMutations elasticMutations;
    private TimingAccessor timing;

    @Override
    protected VertexController getDefaultVertexController() {
        return controller;
    }

    @Override
    protected EdgeController getDefaultEdgeController() {
        return controller;
    }

    @Override
    public void init(UniGraph graph, Configuration configuration) throws Exception {
        String indexName = configuration.getString("graphName", "unipop");

        client = ElasticClientFactory.create(configuration);
        ElasticHelper.createIndex(indexName, client);

        timing = new TimingAccessor();
        elasticMutations = new ElasticMutations(false, client, timing);
        controller =new StarController(graph,client,elasticMutations,indexName,500,false,timing
                /*,new NestedEdgeMapping("created","software",Direction.OUT,"created"),
                new NestedEdgeMapping("knows","person",Direction.OUT,"knows")*/);
    }

    @Override
    public void commit() {
        elasticMutations.commit();
    }

    @Override
    public void close() {
        client.close();
        timing.print();
    }
}
