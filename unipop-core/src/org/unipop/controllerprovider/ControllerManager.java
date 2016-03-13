package org.unipop.controllerprovider;


import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.unipop.controller.EdgeController;
import org.unipop.controller.VertexController;
import org.unipop.structure.*;

import java.util.Iterator;
import java.util.List;

public interface ControllerManager extends VertexController, EdgeController {

    void init(UniGraph graph, Configuration configuration) throws Exception;

    List<BaseElement> properties(List<BaseElement> elements);
    void commit();
    void close();
}
