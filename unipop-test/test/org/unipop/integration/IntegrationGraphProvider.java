package org.unipop.integration;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Map;
import java.util.Set;

public class IntegrationGraphProvider extends AbstractGraphProvider {
    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        return null;
    }

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {

    }

    @Override
    public Set<Class> getImplementations() {
        return null;
    }
}
