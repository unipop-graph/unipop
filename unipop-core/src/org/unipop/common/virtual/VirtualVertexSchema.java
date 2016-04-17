package org.unipop.common.virtual;

import org.unipop.common.schema.base.BaseVertexSchema;
import org.unipop.common.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.util.Map;

public class VirtualVertexSchema extends BaseVertexSchema {


    public VirtualVertexSchema(Map<String, PropertySchema> properties, UniGraph graph) {
        super(properties, false, graph);
    }
}
