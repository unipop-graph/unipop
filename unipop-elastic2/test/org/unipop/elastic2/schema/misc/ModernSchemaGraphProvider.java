package org.unipop.elastic2.schema.misc;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.unipop.controllerprovider.ControllerManagerFactory;
import org.unipop.elastic2.ElasticGraphProvider;
import org.unipop.elastic2.controller.schema.SchemaControllerManager;
import org.unipop.elastic2.controller.schema.helpers.ElasticGraphConfiguration;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by Gilad on 12/10/2015.
 */
public class ModernSchemaGraphProvider extends ElasticGraphProvider {
    public ModernSchemaGraphProvider() throws IOException, ExecutionException, InterruptedException {
    }

    @Override
    public Configuration newGraphConfiguration(String graphName, Class<?> test, String testMethodName, Map<String, Object> configurationOverrides, LoadGraphWith.GraphData loadGraphWith) {
        Configuration configuration = super.newGraphConfiguration(graphName, test, testMethodName, configurationOverrides, loadGraphWith);
        configuration.setProperty("controllerManagerFactory", (ControllerManagerFactory)() -> new SchemaControllerManager());

        ElasticGraphConfiguration elasticConfiguration = new ElasticGraphConfiguration(configuration);
        elasticConfiguration.setElasticGraphSchemaProviderFactory(() -> new ModernGraphElementSchemaProvider(graphName.toLowerCase()));
        elasticConfiguration.setElasticGraphDefaultSearchSize(10000);
        elasticConfiguration.setElasticGraphScrollSize(1000);
        elasticConfiguration.setElasticGraphAggregationsDefaultTermsSize(100000);
        elasticConfiguration.setElasticGraphAggregationsDefaultTermsShardSize(100000);
        elasticConfiguration.setElasticGraphAggregationsDefaultTermsExecutonHint("global_ordinals_hash");
        //elasticConfiguration.setClusterAddress("some-server:9300");
        //elasticConfiguration.setClusterName("some.remote.cluster");

        return elasticConfiguration;
    }
}
