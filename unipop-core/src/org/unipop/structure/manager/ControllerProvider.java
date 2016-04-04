package org.unipop.structure.manager;

import org.apache.commons.configuration.Configuration;
import org.unipop.controller.ElementController;
import org.unipop.structure.UniGraph;

import java.util.Set;

public interface ControllerProvider {
    Set<ElementController> init(UniGraph graph, Configuration configuration) throws Exception;
    void close();
}
