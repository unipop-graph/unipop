package org.elasticgremlin.starQueryHandler;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.elasticgremlin.ElasticGraphGraphProvider;

import java.io.IOException;
import java.util.Map;

public class ModernStarGraphGraphProvider extends ElasticGraphGraphProvider {

    public ModernStarGraphGraphProvider() throws IOException {
    }

    @Override
    public Configuration newGraphConfiguration(String graphName, Class<?> test, String testMethodName, Map<String, Object> configurationOverrides, LoadGraphWith.GraphData loadGraphWith) {
        Configuration configuration = super.newGraphConfiguration(graphName, test, testMethodName, configurationOverrides, loadGraphWith);
        configuration.setProperty("queryHandler", ModernStarGraphQueryHandler.class.getName());
        return configuration;
    }
}
