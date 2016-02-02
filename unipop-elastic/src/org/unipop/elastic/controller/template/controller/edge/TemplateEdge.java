package org.unipop.elastic.controller.template.controller.edge;

import org.apache.commons.lang.NotImplementedException;
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
    private Map<String, String> defaultParams;

    public TemplateEdge(Object id, String label, Map<String, Object> keyValues, Vertex outV, Vertex inV, TemplateEdgeController controller, UniGraph graph, ElasticMutations elasticMutations, String index, Map<String, String> defaultParams) {
        super(id, label, keyValues, outV, inV, controller, graph);
        this.elasticMutations = elasticMutations;
        this.index = index;
        this.defaultParams = defaultParams;
    }

    @Override
    protected boolean shouldAddProperty(String key) {
        return super.shouldAddProperty(key) && !(defaultParams.getOrDefault("outId", "outId").equals(key) ||
                defaultParams.getOrDefault("outLabel", "outLabel").equals(key) ||
                defaultParams.getOrDefault("inId", "inId").equals(key) ||
                defaultParams.getOrDefault("inLabel", "inLabel").equals(key));
    }

    @Override
    protected void innerAddProperty(BaseProperty vertexProperty) {
        throw new NotImplementedException();
    }

    @Override
    protected void innerRemoveProperty(Property property) {
        throw new NotImplementedException();
    }

    @Override
    protected void innerRemove() {
        throw new NotImplementedException();
    }
}
