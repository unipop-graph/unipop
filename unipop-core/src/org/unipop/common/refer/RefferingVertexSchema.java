package org.unipop.common.refer;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.common.schema.BasicSchema;
import org.unipop.common.schema.ElementSchema;
import org.unipop.structure.UniGraph;

import java.util.Map;

public class RefferingVertexSchema extends BasicSchema<Vertex> {

    public RefferingVertexSchema(HierarchicalConfiguration configuration, UniGraph graph){
        super(configuration, graph);
    }

    @Override
    public Vertex createElement(Map<String, Object> properties) {
        return new DeferredVertex(properties, graph);
    }
}
