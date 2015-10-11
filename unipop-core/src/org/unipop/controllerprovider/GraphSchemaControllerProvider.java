package org.unipop.controllerprovider;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;

public abstract class GraphSchemaControllerProvider implements ControllerProvider {
    protected TinkerGraph schema;

    public GraphSchemaControllerProvider() {
        this.schema = TinkerGraph.open();
    }

    @Override
    public VertexController getVertexHandler(Object[] ids) {
        return null;
    }

    @Override
    public VertexController getVertexHandler(Predicates predicates) {
        return null;
    }

    @Override
    public VertexController getVertexHandler(Object vertexId, String vertexLabel, Edge edge, Direction direction) {
        return null;
    }

    @Override
    public VertexController addVertex(Object id, String label, Object[] properties) {
        return null;
    }

    @Override
    public EdgeController getEdgeHandler(Object[] ids) {
        return null;
    }

    @Override
    public EdgeController getEdgeHandler(Predicates predicates) {
        return null;
    }

    @Override
    public EdgeController getEdgeHandler(Vertex vertex, Direction direction, String[] edgeLabels, Predicates predicates) {
        return null;
    }

    @Override
    public EdgeController addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {
        return null;
    }
}
