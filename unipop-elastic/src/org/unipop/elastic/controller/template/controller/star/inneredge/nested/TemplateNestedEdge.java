package org.unipop.elastic.controller.template.controller.star.inneredge.nested;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.controller.EdgeController;
import org.unipop.elastic.controller.template.controller.star.inneredge.TemplateInnerEdge;
import org.unipop.structure.UniGraph;

import java.util.Map;

/**
 * Created by sbarzilay on 02/02/16.
 */
public class TemplateNestedEdge extends TemplateInnerEdge<TemplateNestedEdgeController> {
    public TemplateNestedEdge(Object id, String label, Map<String, Object> keyValues, Vertex outV, Vertex inV, EdgeController controller, UniGraph graph) {
        super(id, label, keyValues, outV, inV, controller, graph);
    }
}
