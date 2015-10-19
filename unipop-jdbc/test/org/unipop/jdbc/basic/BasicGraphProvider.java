package org.unipop.jdbc.basic;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.unipop.jdbc.JdbcGraphProvider;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class BasicGraphProvider extends JdbcGraphProvider {
    public BasicGraphProvider() throws IOException, ExecutionException, InterruptedException {
    }

    @Override
    public Configuration newGraphConfiguration(String graphName, Class<?> test, String testMethodName, Map<String, Object> configurationOverrides, LoadGraphWith.GraphData loadGraphWith) {
        Configuration configuration = super.newGraphConfiguration(graphName, test, testMethodName, configurationOverrides, loadGraphWith);
        configuration.setProperty("controllerManager", BasicJdbcControllerManager.class.getName());
        return configuration;
    }
}
