package org.unipop.elastic.controllerprovider;

import org.apache.commons.configuration.Configuration;
import org.elasticsearch.client.Client;
import org.elasticsearch.script.ScriptService;
import org.unipop.controller.provider.ControllerProvider;
import org.unipop.elastic.controller.edge.ElasticEdgeController;
import org.unipop.elastic.controller.template.controller.vertex.TemplateVertexController;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.UniGraph;

import java.util.HashMap;

/**
 * Created by sbarzilay on 2/10/16.
 */
public class AggsControllerProvider extends ControllerProvider {
    @Override
    public void init(UniGraph graph, Configuration configuration) throws Exception {
        String indexName = "modern";

        Client client = ElasticClientFactory.create(configuration);
        ElasticHelper.createIndex(indexName, client);

        TimingAccessor timing = new TimingAccessor();
        ElasticMutations elasticMutations = new ElasticMutations(false, client, timing);
        ElasticEdgeController edgeController = new ElasticEdgeController(graph, client, elasticMutations, indexName, 0, timing);
//        vertexController = new ElasticVertexController(graph, client, elasticMutations, indexName, 0, timing);
        TemplateVertexController vertexController = new TemplateVertexController(graph, client, elasticMutations, 0, timing, indexName, "dyn_template", ScriptService.ScriptType.FILE, new HashMap<>());

        this.addController(edgeController);
        this.addController(vertexController);
    }
}
