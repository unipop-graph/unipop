package org.unipop.integration;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.test.UnipopGraphProvider;
import test.ElasticGraphProvider;
import test.JdbcGraphProvider;

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

    }

    public String getSchemaConfiguration(LoadGraphWith.GraphData loadGraphWith) {
        if (loadGraphWith != null) {
            switch (loadGraphWith) {
                case MODERN: return "modern.json";
//                case CREW: return CrewConfiguration;
//                case GRATEFUL: return GratefulConfiguration;
            }
        }
        return "modern.json";
    }

    @Override
    public void clear(Graph g, Configuration configuration) throws Exception {
        super.clear(g, configuration);
        this.elasticGraphProvider.clear(g, configuration);
        this.jdbcGraphProvider.clear(g, configuration);
    }
}
