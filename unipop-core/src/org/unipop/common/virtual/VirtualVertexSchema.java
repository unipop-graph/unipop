package org.unipop.common.virtual;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.common.schema.BaseElementSchema;
import org.unipop.structure.UniVertex;
import org.unipop.structure.UniGraph;

import java.util.Map;

public class VirtualVertexSchema extends BaseElementSchema<Vertex> {


    public VirtualVertexSchema(HierarchicalConfiguration configuration, UniGraph graph) {
        super(configuration, graph, graph);
    }

    @Override
    public Vertex createElement(Map<String, Object> properties) {
        return new UniVertex(properties, graph);
    }
}
