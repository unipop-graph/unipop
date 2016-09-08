package org.unipop.virtual;

import org.apache.tinkerpop.gremlin.structure.T;
import org.json.JSONObject;
import org.unipop.query.controller.SourceProvider;
import org.unipop.query.controller.UniQueryController;
import org.unipop.schema.element.ElementSchema;
import org.unipop.structure.UniGraph;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static org.unipop.util.ConversionUtils.getList;

/**
 * Created by sbarzilay on 9/6/16.
 */
public class VirtualSourceProvider implements SourceProvider {
    private UniGraph graph;

    @Override
    public Set<UniQueryController> init(UniGraph graph, JSONObject configuration) throws Exception {
        this.graph = graph;
        Set<ElementSchema> schemas = getList(configuration, "vertices").stream().map(this::createVertexSchema).collect(Collectors.toSet());
        return Collections.singleton(new VirtualController(graph, schemas));
    }

    private VirtualVertexSchema createVertexSchema(JSONObject json) {
        json.accumulate("id", "@" + T.id.getAccessor().toString());
        return new VirtualVertexSchema(json, graph);
    }

    @Override
    public void close() {

    }
}
