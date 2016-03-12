package org.unipop.structure;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.unipop.controller.VertexController;
import org.unipop.controllerprovider.ControllerManager;

/**
 * Created by sbarzilay on 3/9/16.
 */
public class UniDelayedVertex extends UniVertex {
    public UniDelayedVertex(Object id, String label, ControllerManager manager, UniGraph graph) {
        super(id, label, null, manager, graph);
    }

    @Override
    protected void innerAddProperty(BaseVertexProperty vertexProperty) {
        getManager().addPropertyToVertex(this, vertexProperty);
    }

    @Override
    protected void innerRemoveProperty(Property property) {
        getManager().removePropertyFromVertex(this, property);
    }

    @Override
    protected void innerRemove() {
        getManager().removeVertex(this);
    }
}
