package org.unipop.controller.virtualvertex;

import org.unipop.structure.BaseVertex;
import org.unipop.structure.BaseVertexProperty;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.unipop.structure.*;

public class VirtualVertex extends BaseVertex {
    protected VirtualVertex(Object id, String label, UniGraph graph, Object[] keyValues) {
        super(id, label, graph, keyValues);
    }

    @Override
    protected void innerAddProperty(BaseVertexProperty vertexProperty) {

    }

    @Override
    protected void innerRemoveProperty(Property property) {

    }

    @Override
    protected void innerRemove() {

    }
}
