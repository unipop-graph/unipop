package org.unipop.structure;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.unipop.controller.VertexController;
import org.unipop.controller.provider.ControllerProvider;

/**
 * Created by sbarzilay on 3/9/16.
 */
public class UniDelayedVertex extends UniVertex {
    public UniDelayedVertex(Object id, String label, VertexController controller, UniGraph graph) {
        super(id, label, null, controller, graph);
    }

    @Override
    protected void innerAddProperty(BaseVertexProperty vertexProperty) {
        controller.addPropertyToVertex(this, vertexProperty);
    }

    @Override
    protected void innerRemoveProperty(Property property) {
        controller.removePropertyFromVertex(this, property);
    }

    @Override
    protected void innerRemove() {
        controller.removeVertex(this);
    }
}
