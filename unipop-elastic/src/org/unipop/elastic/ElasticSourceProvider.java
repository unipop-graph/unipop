package org.unipop.elastic;

import com.google.common.collect.Sets;
import org.apache.commons.configuration.ConfigurationMap;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.elasticsearch.client.Client;
import org.unipop.common.schema.SchemaSet;
import org.unipop.elastic.document.DocumentController;
import org.unipop.query.controller.UniQueryController;
import org.unipop.query.controller.SourceProvider;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.UniGraph;

import java.util.Map;
import java.util.Set;

public class ElasticSourceProvider implements SourceProvider {

    private Client client;

    @Override
    public Set<UniQueryController> init(UniGraph graph, HierarchicalConfiguration configuration) throws Exception {
        SubnodeConfiguration elasticConfiguration = configuration.configurationAt("source.elastic");

        this.client = createClient(elasticConfiguration);
        elasticConfiguration.configurationsAt("vertices").forEach(vertex -> {
            vertex.

            SchemaBuilder.createPropertySchema(configuration.getProperty(T.id.getAccessor()));
        });
        addPropertySchema(T.id.getAccessor(), configuration.getProperty(T.id.getAccessor()));
        addPropertySchema(T.label.getAccessor(), configuration.getProperty(T.label.getAccessor()));
        ConfigurationMap properties = new ConfigurationMap(configuration.subset("properties"));
        for(Map.Entry<Object, Object> property : properties.entrySet()) {
            addPropertySchema(property.getKey().toString(), property.getValue());
        }
        this.dynamicProperties = configuration.getBoolean("dynamicProperties", true);

        String indexName = configuration.getString("graphName", "unipop");

        this.client = ElasticClientFactory.create(configuration);
        ElasticHelper.createIndex(indexName, this.client);

        TimingAccessor timing = new TimingAccessor();
        SchemaSet<ElasticElementSchema> schemas = null;//new SchemaSet<>()
        ElasticMutations elasticMutations = new ElasticMutations(false, this.client, timing);
        DocumentController documentController = new DocumentController(this.client, elasticMutations, schemas);

        return Sets.newHashSet(documentController);
    }

    private Client createClient(SubnodeConfiguration elasticConfiguration) {
        return null;
    }

    @Override
    public void close() {
        client.close();
    }
}
