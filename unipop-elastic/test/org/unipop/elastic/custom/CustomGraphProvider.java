package org.unipop.elastic.custom;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.unipop.elastic.ElasticGraphProvider;
import org.unipop.elastic.basic.BasicControllerProvider;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class CustomGraphProvider extends ElasticGraphProvider {


    public CustomGraphProvider() throws IOException, ExecutionException, InterruptedException {
    }

    @Override
    public Configuration newGraphConfiguration(String graphName, Class<?> test, String testMethodName, Map<String, Object> configurationOverrides, LoadGraphWith.GraphData loadGraphWith) {
        Configuration configuration = super.newGraphConfiguration(graphName, test, testMethodName, configurationOverrides, loadGraphWith);
        if(loadGraphWith != null && loadGraphWith.equals(LoadGraphWith.GraphData.MODERN))
            configuration.setProperty("controllerProvider", ModernGraphControllerProvider.class.getName());
        else configuration.setProperty("controllerProvider", BasicControllerProvider.class.getName());
        return configuration;
    }
}
