package org.unipop.controllerprovider;


import org.apache.commons.configuration.Configuration;
import org.unipop.controller.EdgeController;
import org.unipop.controller.VertexController;
import org.unipop.structure.UniGraph;

public interface ControllerManager extends VertexController, EdgeController {

    void init(UniGraph graph, Configuration configuration) throws Exception;
    void commit();
    void close();
}
