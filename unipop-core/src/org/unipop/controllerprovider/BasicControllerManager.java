package org.unipop.controllerprovider;

import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;

import java.util.Iterator;
import java.util.Map;

public abstract class BasicControllerManager implements ControllerManager {

    protected abstract VertexController getDefaultVertexController();
    protected abstract EdgeController getDefaultEdgeController();

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates, MutableMetrics metrics) {
        return getDefaultVertexController().vertices(predicates, metrics);
    }

    @Override
    public BaseVertex fromEdge(Direction direction, Object vertexId, String vertexLabel) {
        return getDefaultVertexController().fromEdge(direction, vertexId, vertexLabel);
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Map<String, Object> properties) {
        return getDefaultVertexController().addVertex(id, label, properties);
    }

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates, MutableMetrics metrics) {
        return getDefaultEdgeController().edges(predicates, metrics);
    }

    @Override
    public Iterator<BaseEdge> fromVertex(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, MutableMetrics metrics) {
        return getDefaultEdgeController().fromVertex(vertices, direction, edgeLabels, predicates, metrics);
    }

    @Override
    public BaseEdge addEdge(Object edgeId, String label, BaseVertex outV, BaseVertex inV, Map<String, Object> properties) {
        return getDefaultEdgeController().addEdge(edgeId, label, outV, inV, properties);
    }
}
