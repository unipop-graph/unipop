package org.unipop.elastic.controller.template.controller.edge;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseProperty;
import org.unipop.structure.UniGraph;

import java.util.Map;

/**
 * Created by sbarzilay on 02/02/16.
 */
public class TemplateEdge extends BaseEdge {
    private ElasticMutations elasticMutations;
    private String index;

    public TemplateEdge(Object id, String label, Map<String, Object> keyValues, Vertex outV, Vertex inV, TemplateEdgeController controller, UniGraph graph, ElasticMutations elasticMutations, String index) {
        super(id, label, keyValues, outV, inV, controller, graph);
        this.elasticMutations = elasticMutations;
        this.index = index;
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
