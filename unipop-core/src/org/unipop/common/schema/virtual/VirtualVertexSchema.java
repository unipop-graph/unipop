package org.unipop.common.schema.virtual;

import org.unipop.common.schema.base.BaseVertexSchema;
import org.unipop.common.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.util.List;

public class VirtualVertexSchema extends BaseVertexSchema {

    public VirtualVertexSchema(List<PropertySchema> properties, UniGraph graph) {
        super(properties, graph);
    }
}
