package org.unipop.controllerprovider;

import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.util.Iterator;
import java.util.Map;

public abstract class BasicControllerManager implements ControllerManager {

    protected abstract VertexController getDefaultVertexController();
    protected abstract EdgeController getDefaultEdgeController();

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates) {
        return getDefaultEdgeController().edges(predicates);
    }

    @Override
    public Iterator<BaseEdge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        return getDefaultEdgeController().edges(vertices, direction, edgeLabels, predicates);
    }

    @Override
    public long edgeCount(Predicates predicates) {
        return getDefaultEdgeController().edgeCount(predicates);
    }

    @Override
    public long edgeCount(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        return getDefaultEdgeController().edgeCount(vertices, direction, edgeLabels, predicates);
    }

    @Override
    public Map<String, Object> edgeGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        return getDefaultEdgeController().edgeGroupBy(predicates, keyTraversal, valuesTraversal, reducerTraversal);
    }

    @Override
    public Map<String, Object> edgeGroupBy(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        return getDefaultEdgeController().edgeGroupBy(vertices, direction, edgeLabels, predicates, keyTraversal, valuesTraversal, reducerTraversal);
    }

    @Override
    public BaseEdge addEdge(Object edgeId, String label, BaseVertex outV, BaseVertex inV, Map<String, Object> properties) {
        return getDefaultEdgeController().addEdge(edgeId, label, outV, inV, properties);
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates) {
        return getDefaultVertexController().vertices(predicates);
    }

    @Override
    public BaseVertex vertex(Direction direction, Object vertexId, String vertexLabel) {
        return getDefaultVertexController().vertex(direction, vertexId, vertexLabel);
    }

    @Override
    public long vertexCount(Predicates predicates) {
        return getDefaultVertexController().vertexCount(predicates);
    }

    @Override
    public Map<String, Object> vertexGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        return getDefaultVertexController().vertexGroupBy(predicates, keyTraversal, valuesTraversal, reducerTraversal);
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Map<String, Object> properties) {
        return getDefaultVertexController().addVertex(id, label, properties);
    }

    @Override
    public void init(Map<String, Object> conf, UniGraph graph) throws Exception {
        throw new NotImplementedException();
    }
}
