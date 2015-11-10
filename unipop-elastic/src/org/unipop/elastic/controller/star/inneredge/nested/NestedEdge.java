package org.unipop.elastic.controller.star.inneredge.nested;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.elastic.controller.star.ElasticStarVertex;
import org.unipop.elastic.controller.star.inneredge.InnerEdge;
import org.unipop.elastic.controller.star.inneredge.InnerEdgeController;
import org.unipop.structure.BaseProperty;

import java.util.Map;

public class NestedEdge extends InnerEdge {

    private ElasticStarVertex starVertex;

    public NestedEdge(ElasticStarVertex starVertex, Object edgeId, String edgeLabel, InnerEdgeController mapping, Vertex outVertex, Vertex inVertex, Map<String, Object> keyValues) {
        super(starVertex, edgeId, edgeLabel, mapping, outVertex, inVertex, keyValues);
        this.starVertex = starVertex;

    }

    @Override
    protected void innerRemoveProperty(Property property) {
        properties.remove(property.key());
        starVertex.update();
    }

    @Override
    protected void innerRemove() {
        starVertex.removeInnerEdge(this);
        starVertex.update();
    }

    @Override
    protected void innerAddProperty(BaseProperty vertexProperty) {
        properties.put(vertexProperty.key(), vertexProperty);
        starVertex.update();
    }
}
