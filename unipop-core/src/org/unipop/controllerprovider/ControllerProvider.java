package org.unipop.controllerprovider;


import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.structure.UniGraph;

import java.io.IOException;

public interface ControllerProvider {

    void init(UniGraph graph, Configuration configuration) throws IOException;
    void commit();
    void printStats();
    void close();

    VertexController getVertexHandler(Object[] ids);
    VertexController getVertexHandler(Predicates predicates);
    VertexController getVertexHandler(Object vertexId, String vertexLabel, Edge edge, Direction direction);
    VertexController addVertex(Object id, String label, Object[] properties);

    EdgeController getEdgeHandler(Object[] ids);
    EdgeController getEdgeHandler(Predicates predicates);
    EdgeController getEdgeHandler(Vertex vertex, Direction direction, String[] edgeLabels, Predicates predicates);
    EdgeController addEdge(Object edgeId, String label,Vertex outV, Vertex inV, Object[] properties);

}
