package org.unipop.query.controller;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.unipop.structure.UniGraph;

import java.util.Set;

public interface SourceProvider {
    Set<UniQueryController> init(UniGraph graph, HierarchicalConfiguration configuration) throws Exception;
    void close();
}
