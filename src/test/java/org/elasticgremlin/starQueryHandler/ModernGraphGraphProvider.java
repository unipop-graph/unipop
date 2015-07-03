package org.elasticgremlin.starQueryHandler;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.elasticgremlin.ElasticGraphGraphProvider;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ModernGraphGraphProvider extends ElasticGraphGraphProvider {


    public ModernGraphGraphProvider() throws IOException, ExecutionException, InterruptedException {
    }

    @Override
    public Configuration newGraphConfiguration(String graphName, Class<?> test, String testMethodName, Map<String, Object> configurationOverrides, LoadGraphWith.GraphData loadGraphWith) {
        Configuration configuration = super.newGraphConfiguration(graphName, test, testMethodName, configurationOverrides, loadGraphWith);
        configuration.setProperty("queryHandler", ModernGraphQueryHandler.class.getName());
        return configuration;
    }
}
