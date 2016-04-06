package org.unipop.common.test;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;

public interface TestInitializer {
    void clear(Graph g, Configuration configuration);
}
