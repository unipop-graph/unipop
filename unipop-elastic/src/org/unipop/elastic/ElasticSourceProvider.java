package org.unipop.elastic;

import com.google.common.collect.Sets;
import io.searchbox.client.JestClient;
import org.json.JSONObject;
import org.unipop.elastic.common.ElasticClient;
import org.unipop.elastic.document.DocumentController;
import org.unipop.elastic.document.schema.DocVertexSchema;
import org.unipop.elastic.document.schema.builder.DocEdgeBuilder;
import org.unipop.elastic.document.schema.builder.DocVertexBuilder;
import org.unipop.common.util.SchemaSet;
import org.unipop.query.controller.SourceProvider;
import org.unipop.query.controller.UniQueryController;
import org.unipop.structure.UniGraph;

import java.util.Set;

import static org.unipop.common.util.ConversionUtils.getList;

public class ElasticSourceProvider implements SourceProvider {

    private ElasticClient client;
    private UniGraph graph;

    @Override
    public Set<UniQueryController> init(UniGraph graph, JSONObject configuration) throws Exception {
        this.graph = graph;
        this.client = new ElasticClient(configuration);

        SchemaSet schemas = new SchemaSet();
        getList(configuration, "vertices").forEach(vertexJson -> schemas.add(createVertexSchema(vertexJson)));
        getList(configuration, "edges").forEach(edgeJson -> schemas.add(createEdgeSchema(edgeJson).build()));

        return createControllers(schemas);
    }

    public DocVertexSchema createVertexSchema(JSONObject vertexJson) {
        return new DocVertexBuilder(vertexJson, graph).build();
    }

    public DocEdgeBuilder createEdgeSchema(JSONObject edgeJson) {
        return new DocEdgeBuilder(edgeJson, graph);
    }

    public Set<UniQueryController> createControllers(SchemaSet schemas) {
        DocumentController documentController = new DocumentController(client, schemas, graph);
        return Sets.newHashSet(documentController);
    }

    @Override
    public void close() {
        client.getClient().shutdownClient();
    }
}