package org.unipop.controller.virtualvertex;

import org.unipop.structure.BaseVertex;
import org.unipop.structure.BaseVertexProperty;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.unipop.structure.*;

import java.util.Map;

public class VirtualVertex extends BaseVertex {
    protected VirtualVertex(Object id, String label, Map<String, Object> keyValues, VirtualVertexController controller, UniGraph graph) {
        super(id, label, keyValues, controller, graph);
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
