package org.unipop.elastic.controller.star.inneredge.nested;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.elastic.controller.star.ElasticStarVertex;
import org.unipop.elastic.controller.star.inneredge.InnerEdge;
import org.unipop.structure.BaseProperty;

public class NestedEdge extends InnerEdge<NestedEdgeController> {

    private ElasticStarVertex starVertex;

    public NestedEdge(ElasticStarVertex starVertex, Object edgeId, String edgeLabel, NestedEdgeController mapping, Vertex outVertex, Vertex inVertex) {
        super(starVertex, edgeId, edgeLabel, mapping, outVertex, inVertex);
        this.starVertex = starVertex;

    }

    @Override
    protected boolean shouldAddProperty(String key) {
        return super.shouldAddProperty(key) && getInnerEdgeController().shouldAddProperty(key);
    }

    @Override
    protected void innerRemoveProperty(Property property) {
        properties.remove(property.key());
        starVertex.update(false);
    }

    @Override
    protected void innerRemove() {
        starVertex.removeInnerEdge(this);
        starVertex.update(true);
    }

    @Override
    protected void innerAddProperty(BaseProperty vertexProperty) {
        properties.put(vertexProperty.key(), vertexProperty);
        starVertex.update(false);
    }
}
