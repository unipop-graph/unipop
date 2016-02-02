package org.unipop.elastic.controller.template.controller.vertex;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.BaseVertexProperty;
import org.unipop.structure.UniGraph;

import java.util.Map;

/**
 * Created by sbarzilay on 02/02/16.
 */
public class TemplateVertex<T extends TemplateVertexController> extends BaseVertex<T>{
    private ElasticMutations elasticMutations;
    private String index;

    protected TemplateVertex(Object id, String label, Map<String, Object> keyValues, T controller, UniGraph graph, ElasticMutations elasticMutations, String index) {
        super(id, label, keyValues, controller, graph);
        this.elasticMutations = elasticMutations;
        this.index = index;
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
