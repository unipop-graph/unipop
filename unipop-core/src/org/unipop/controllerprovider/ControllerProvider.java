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

public interface ControllerProvider extends VertexController, EdgeController {

    void init(UniGraph graph, Configuration configuration) throws IOException;
    void commit();
    void printStats();
    void close();
}
