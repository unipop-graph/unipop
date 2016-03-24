package org.unipop.structure.manager;

import org.apache.commons.configuration.Configuration;
import org.unipop.controller.Controller;
import org.unipop.structure.UniGraph;

import java.util.Set;

public interface ControllerProvider {
    Set<Controller> init(UniGraph graph, Configuration configuration) throws Exception;
    void close();
}
