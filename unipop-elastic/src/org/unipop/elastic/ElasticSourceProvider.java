package org.unipop.elastic;

import com.google.common.collect.Sets;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.elastic.common.ElasticClient;
import org.unipop.elastic.document.DocumentController;
import org.unipop.elastic.document.DocumentSchema;
import org.unipop.elastic.document.schema.DocEdgeSchema;
import org.unipop.elastic.document.schema.DocVertexSchema;
import org.unipop.elastic.document.schema.property.IndexPropertySchema;
import org.unipop.elastic.document.schema.property.InnerPropertySchema;
import org.unipop.elastic.document.schema.property.KeywordPropertySchema;
import org.unipop.query.controller.SourceProvider;
import org.unipop.query.controller.UniQueryController;
import org.unipop.schema.property.PropertySchema;
import org.unipop.structure.traversalfilter.TraversalFilter;
import org.unipop.structure.UniGraph;
import org.unipop.util.ConversionUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.unipop.util.ConversionUtils.getList;

public class ElasticSourceProvider implements SourceProvider {

    private ElasticClient client;
    private UniGraph graph;

    @Override
    public Set<UniQueryController> init(UniGraph graph, JSONObject configuration, TraversalFilter traversalFilter) throws Exception {
        this.graph = graph;

        List<String> addresses = ConversionUtils.toStringList(configuration, "addresses");
        this.client = new ElasticClient(addresses);

        Set<DocumentSchema> schemas = new HashSet<>();
        for (JSONObject json : getList(configuration, "vertices")) {
            schemas.add(createVertexSchema(json));
        }
        for (JSONObject json : getList(configuration, "edges")) {
            schemas.add(createEdgeSchema(json));
        }

        DocumentController documentController = new DocumentController(schemas, client, graph, traversalFilter);
        return Sets.newHashSet(documentController);
    }

    @Override
    public List<PropertySchema.PropertySchemaBuilder> providerBuilders() {
        return Arrays.asList(new KeywordPropertySchema.Builder(), new IndexPropertySchema.Builder(), new InnerPropertySchema.Builder());
    }

    protected DocVertexSchema createVertexSchema(JSONObject vertexJson) throws JSONException {
        return new DocVertexSchema(vertexJson, client, graph);
    }

    protected DocEdgeSchema createEdgeSchema(JSONObject edgeJson) throws JSONException {
        return new DocEdgeSchema(edgeJson, client, graph);
    }

    @Override
    public void close() {
        client.close();
    }
}