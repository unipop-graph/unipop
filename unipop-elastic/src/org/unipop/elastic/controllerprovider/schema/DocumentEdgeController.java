package org.unipop.elastic.controllerprovider.schema;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.unipop.controller.EdgeController;
import org.unipop.elastic.schema.ElasticVertexSchema;
import org.unipop.structure.UniGraph;

import java.util.Map;

public class DocumentEdgeController extends DocumentSchema<Edge, EdgeController> {
    private final ElasticVertexSchema outVertexSchema;
    private final ElasticVertexSchema inVertexSchema;

    public DocumentEdgeController(ElasticVertexSchema outVertexSchema, ElasticVertexSchema inVertexSchema, HierarchicalConfiguration configuration, UniGraph graph) {
        super(configuration, graph);
        this.outVertexSchema = outVertexSchema;
        this.inVertexSchema = inVertexSchema;
    }

    @Override
    protected Edge createElement(Object id, String label, Map<String, Object> properties, EdgeController controller) {
        outVertexSchema.createVertex()
        return null;
    }
}
