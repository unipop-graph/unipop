package org.unipop.elastic.controllerprovider;

import org.apache.commons.configuration.Configuration;
import org.elasticsearch.client.Client;
import org.elasticsearch.script.ScriptService;
import org.unipop.structure.manager.ControllerProvider;
import org.unipop.elastic.controller.template.controller.edge.TemplateEdgeController;
import org.unipop.elastic.controller.template.controller.vertex.TemplateVertexController;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.UniGraph;

import java.util.HashMap;

/**
 * Created by sbarzilay on 02/02/16.
 */
public class TemplateControllerProvider extends ControllerProvider
{
    @Override
    public void init(UniGraph graph, Configuration configuration) throws Exception {
        String indexName = "index";

        Client client = ElasticClientFactory.create(configuration);
        ElasticHelper.createIndex(indexName, client);

        TimingAccessor timing = new TimingAccessor();
        ElasticMutations elasticMutations = new ElasticMutations(false, client, timing);
        TemplateEdgeController edgeController = new TemplateEdgeController(graph, client, elasticMutations, indexName, 0, timing, "test4", ScriptService.ScriptType.FILE);
        TemplateVertexController vertexController = new TemplateVertexController(graph, client, elasticMutations, 0, timing, indexName, "test3", ScriptService.ScriptType.FILE, new HashMap<>());

        this.addController(edgeController);
        this.addController(vertexController);
    }
}
