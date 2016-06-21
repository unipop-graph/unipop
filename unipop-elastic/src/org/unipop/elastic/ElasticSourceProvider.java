package org.unipop.elastic;

import com.google.common.collect.Sets;
import org.elasticsearch.client.Client;
import org.json.JSONObject;
import org.unipop.elastic.common.ElasticClientFactory;
import org.unipop.elastic.document.DocumentController;
import org.unipop.elastic.schemaBuilder.DocEdgeBuilder;
import org.unipop.elastic.schemaBuilder.DocVertexBuilder;
import org.unipop.common.util.SchemaSet;
import org.unipop.query.controller.SourceProvider;
import org.unipop.query.controller.UniQueryController;
import org.unipop.structure.UniGraph;

import java.util.Set;

import static org.unipop.common.util.ConversionUtils.getList;

public class ElasticSourceProvider implements SourceProvider {

    private Client client;

    @Override
    public Set<UniQueryController> init(UniGraph graph, JSONObject configuration) throws Exception {
        this.client = ElasticClientFactory.create(configuration);

        SchemaSet schemas = new SchemaSet();
        getList(configuration, "vertices").forEach(vertexJson -> schemas.add(new DocVertexBuilder(vertexJson, client, graph).build()));
        getList(configuration, "edges").forEach(edgeJson -> schemas.add(new DocEdgeBuilder(edgeJson, client, graph).build()));
        DocumentController documentController = new DocumentController(this.client, schemas, graph);
        return Sets.newHashSet(documentController);
    }

    @Override
    public void close() {
        client.close();
    }
}
