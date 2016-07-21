package org.unipop.integration;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.test.UnipopGraphProvider;
import test.ElasticGraphProvider;
import test.JdbcGraphProvider;

import java.net.URL;
import java.util.Map;

public class IntegrationGraphProvider extends UnipopGraphProvider {

    private final ElasticGraphProvider elasticGraphProvider;
    private final JdbcGraphProvider jdbcGraphProvider;

    public IntegrationGraphProvider() throws Exception {
        this.jdbcGraphProvider = new JdbcGraphProvider();
        this.elasticGraphProvider = new ElasticGraphProvider();
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        Map<String, Object> baseConfiguration = super.getBaseConfiguration(graphName, test, testMethodName, loadGraphWith);
        baseConfiguration.put("bulk.max", 1000);
        baseConfiguration.put("bulk.start", 10);
        baseConfiguration.put("bulk.multiplier", 10);

        String configurationFile = getSchemaConfiguration(loadGraphWith);
        if(configurationFile != null) {
            URL jdbcUrl = this.getClass().getResource("/configuration/jdbc/" + configurationFile);
            URL elasticUrl = this.getClass().getResource("/configuration/elastic/" + configurationFile);
            baseConfiguration.put("providers", jdbcUrl.getFile() + ";" + elasticUrl.getFile());
        }
        else {
            URL defaultSchema = this.getClass().getResource("/configuration/basic.json");
            baseConfiguration.put("providers", defaultSchema.getFile());
        }

        return baseConfiguration;
    }

    public String getSchemaConfiguration(LoadGraphWith.GraphData loadGraphWith) {
        if (loadGraphWith != null) {
            switch (loadGraphWith) {
                case MODERN:
                    return "modern.json";
//                case CREW: return CrewConfiguration;
//                case GRATEFUL: return GratefulConfiguration;
            }
        }
        return null;
    }

    @Override
    public void clear(Graph g, Configuration configuration) throws Exception {
        super.clear(g, configuration);
        this.elasticGraphProvider.clear(g, configuration);
        this.jdbcGraphProvider.clear(g, configuration);
    }

    @Override
    public Object convertId(Object id, Class<? extends Element> c) {
        return id.toString();
    }
}
