package org.unipop.query.controller;

import org.apache.commons.configuration.Configuration;
import org.unipop.structure.UniGraph;

import java.util.Set;

public interface ControllerProvider {
    Set<UniQueryController> init(UniGraph graph, Configuration configuration) throws Exception;
    void close();
}
