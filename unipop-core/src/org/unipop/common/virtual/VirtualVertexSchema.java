package org.unipop.common.virtual;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.common.schema.BasicSchema;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.util.Map;

public class VirtualVertexSchema extends BasicSchema<Vertex> {


    public VirtualVertexSchema(HierarchicalConfiguration configuration, UniGraph graph) {
        super(configuration, graph);
    }

    @Override
    public Vertex createElement(Map<String, Object> properties) {
        return new BaseVertex(properties, graph);
    }
}
