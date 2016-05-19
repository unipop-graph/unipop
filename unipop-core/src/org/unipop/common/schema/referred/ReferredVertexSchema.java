package org.unipop.common.schema.referred;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.common.schema.base.BaseVertexSchema;
import org.unipop.common.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.util.List;
import java.util.Map;

public class ReferredVertexSchema extends BaseVertexSchema {


    public ReferredVertexSchema(List<PropertySchema> properties, UniGraph graph) {
        super(properties, graph);
    }

    @Override
    public Vertex fromFields(Map<String, Object> fields) {
        Map<String, Object> properties = getProperties(fields);
        return new DeferredVertex(properties, graph);
    }
}
