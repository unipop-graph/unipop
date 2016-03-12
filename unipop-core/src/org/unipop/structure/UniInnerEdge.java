package org.unipop.structure;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.controller.EdgeController;
import org.unipop.controller.InnerEdgeController;

/**
 * Created by sbarzilay on 3/10/16.
 */
public class UniInnerEdge extends BaseEdge{
    private final InnerEdgeController innerEdgeController;
    private UniStarVertex starVertex;

    public UniInnerEdge(UniStarVertex starVertex, Object edgeId, String edgeLabel, InnerEdgeController innerEdgeController, Vertex outVertex, Vertex inVertex) {
        super(edgeId, edgeLabel, null, outVertex, inVertex, ((EdgeController) starVertex.getManager()), starVertex.getGraph());
        this.innerEdgeController = innerEdgeController;
        this.starVertex = starVertex;
    }

    public InnerEdgeController getInnerEdgeController() {
        return innerEdgeController;
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
        if (vertexProperty != null) {
            properties.put(vertexProperty.key(), vertexProperty);
            starVertex.update(false);
        }
    }
}
