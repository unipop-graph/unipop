package org.unipop.controllerprovider;


import org.apache.commons.configuration.Configuration;
import org.unipop.controller.EdgeController;
import org.unipop.controller.VertexController;
import org.unipop.structure.UniGraph;

import java.io.IOException;

public interface ControllerManager extends VertexController, EdgeController {

    void init(UniGraph graph, Configuration configuration) throws IOException;
    void commit();
    void close();
}
