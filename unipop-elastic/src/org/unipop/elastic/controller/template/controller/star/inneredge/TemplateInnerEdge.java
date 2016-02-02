package org.unipop.elastic.controller.template.controller.star.inneredge;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.controller.EdgeController;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseProperty;
import org.unipop.structure.UniGraph;

import java.util.Map;

/**
 * Created by sbarzilay on 02/02/16.
 */
public abstract class TemplateInnerEdge<T extends TemplateInnerEdgeController> extends BaseEdge {
    public TemplateInnerEdge(Object id, String label, Map<String, Object> keyValues, Vertex outV, Vertex inV, EdgeController controller, UniGraph graph) {
        super(id, label, keyValues, outV, inV, controller, graph);
    }

    @Override
    protected void innerAddProperty(BaseProperty vertexProperty) {

    }

    @Override
    protected void innerRemoveProperty(Property property) {

    }

    @Override
    protected void innerRemove() {

    }
}
