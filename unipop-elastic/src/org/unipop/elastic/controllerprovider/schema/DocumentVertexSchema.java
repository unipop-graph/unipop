package org.unipop.elastic.controllerprovider.schema;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.controller.ElementController;
import org.unipop.controller.VertexController;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.util.Map;

public class DocumentVertexSchema extends DocumentSchema<Vertex, VertexController> {
    public DocumentVertexSchema(HierarchicalConfiguration configuration, UniGraph graph) {
        super(configuration, graph);
    }

    @Override
    protected Vertex createElement(Object id, String label, Map<String, Object> properties, VertexController controller) {
        return new BaseVertex<>(id, label, properties, controller, graph);
    }
}
