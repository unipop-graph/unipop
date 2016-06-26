package org.unipop.elastic;

import com.google.common.collect.Sets;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.common.util.ConversionUtils;
import org.unipop.common.util.SchemaSet;
import org.unipop.elastic.common.ElasticClient;
import org.unipop.elastic.document.DocumentController;
import org.unipop.elastic.document.schema.builder.DocEdgeBuilder;
import org.unipop.elastic.document.schema.builder.DocVertexBuilder;
import org.unipop.query.controller.SourceProvider;
import org.unipop.query.controller.UniQueryController;
import org.unipop.structure.UniGraph;

import java.util.List;
import java.util.Set;

import static org.unipop.common.util.ConversionUtils.getList;

public class ElasticSourceProvider implements SourceProvider {

    private ElasticClient client;
    private UniGraph graph;

    @Override
    public Set<UniQueryController> init(UniGraph graph, JSONObject configuration) throws Exception {
        this.graph = graph;

        List<String> addresses = ConversionUtils.toStringList(configuration.getJSONArray("addresses"));
        this.client = new ElasticClient(addresses);

        SchemaSet schemas = new SchemaSet();
        for(JSONObject json : getList(configuration, "vertices")) {
            schemas.add(createVertexSchema(json).build());
        }
        for(JSONObject json : getList(configuration, "edges")) {
            schemas.add(createEdgeSchema(json).build());
        }

        return createControllers(schemas);
    }

    protected DocVertexBuilder createVertexSchema(JSONObject vertexJson) throws JSONException {
        return new DocVertexBuilder(vertexJson, graph);
    }

    protected DocEdgeBuilder createEdgeSchema(JSONObject edgeJson) throws JSONException {
        return new DocEdgeBuilder(edgeJson, graph);
    }

    protected Set<UniQueryController> createControllers(SchemaSet schemas) {
        DocumentController documentController = new DocumentController(client, schemas, graph);
        return Sets.newHashSet(documentController);
    }

    @Override
    public void close() {
        client.getClient().shutdownClient();
    }
}