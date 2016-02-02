package org.unipop.elastic.controllermanagers;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.elasticsearch.client.Client;
import org.elasticsearch.script.ScriptService;
import org.unipop.controller.EdgeController;
import org.unipop.controller.VertexController;
import org.unipop.controllerprovider.BasicControllerManager;
import org.unipop.elastic.controller.template.controller.edge.TemplateEdgeController;
import org.unipop.elastic.controller.template.controller.vertex.TemplateVertexController;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.UniGraph;

/**
 * Created by sbarzilay on 02/02/16.
 */
public class TemplateControllerManager extends BasicControllerManager
{
    TemplateVertexController vertexController;
    TemplateEdgeController edgeController;
    private Client client;
    private ElasticMutations elasticMutations;
    private TimingAccessor timing;

    @Override
    protected VertexController getDefaultVertexController() {
        return vertexController;
    }

    @Override
    protected EdgeController getDefaultEdgeController() {
        return edgeController;
    }

    @Override
    public void init(UniGraph graph, Configuration configuration) throws Exception {
        String indexName = "index";

        client = ElasticClientFactory.create(configuration);
        ElasticHelper.createIndex(indexName, client);

        timing = new TimingAccessor();
        elasticMutations = new ElasticMutations(false, client, timing);
        edgeController = new TemplateEdgeController(graph, client, elasticMutations, indexName, 0, timing, "test4", ScriptService.ScriptType.FILE);
        vertexController = new TemplateVertexController(graph,client, elasticMutations, 0, timing, indexName, "test3", ScriptService.ScriptType.FILE, T.id.getAccessor(), "ids", "name", "names");
    }

    @Override
    public void commit() {
        elasticMutations.commit();
    }

    @Override
    public void close() {
        client.close();
    }
}
