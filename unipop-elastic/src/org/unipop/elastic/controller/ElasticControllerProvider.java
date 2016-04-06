package org.unipop.elastic.controller;

import com.google.common.collect.Sets;
import org.apache.commons.configuration.Configuration;
import org.elasticsearch.client.Client;
import org.unipop.common.schema.SchemaSet;
import org.unipop.controller.ElementController;
import org.unipop.controller.manager.ControllerProvider;
import org.unipop.elastic.controller.helpers.ElasticClientFactory;
import org.unipop.elastic.controller.helpers.ElasticHelper;
import org.unipop.elastic.controller.helpers.ElasticMutations;
import org.unipop.elastic.controller.helpers.TimingAccessor;
import org.unipop.elastic.schema.ElasticElementSchema;
import org.unipop.structure.UniGraph;

import java.util.Set;

public class ElasticControllerProvider implements ControllerProvider {

    private Client client;

    @Override
    public Set<ElementController> init(UniGraph graph, Configuration configuration) throws Exception {
        String indexName = configuration.getString("graphName", "unipop");

        this.client = ElasticClientFactory.create(configuration);
        ElasticHelper.createIndex(indexName, client);

        TimingAccessor timing = new TimingAccessor();
        SchemaSet<ElasticElementSchema> schemas = null;//new SchemaSet<>()
        ElasticMutations elasticMutations = new ElasticMutations(false, client, timing);
        DocumentController documentController = new DocumentController(client, elasticMutations, schemas);

        return Sets.newHashSet(documentController);
    }

    @Override
    public void close() {
        client.close();
    }
}
