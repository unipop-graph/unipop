package org.unipop.integration;

import org.apache.commons.configuration2.Configuration;
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
        baseConfiguration.put("bulk.graph", 10);
        baseConfiguration.put("bulk.multiplier", 10);

        // The migrated ConfigurationControllerManager treats "providers" as a single FOLDER and
        // walks it (Files.walk) for provider JSONs — the pre-migration semicolon-joined file list
        // is no longer supported. Federation is therefore expressed as one folder holding both the
        // jdbc and elastic provider configs (integration/modern/{jdbc,elastic}.json); a default
        // folder holds the elastic-only basic config.
        String schemaFolder = getSchemaConfiguration(loadGraphWith);
        URL providersFolder = this.getClass().getResource(schemaFolder);
        baseConfiguration.put("providers", providersFolder.getFile());

        return baseConfiguration;
    }

    public String getSchemaConfiguration(LoadGraphWith.GraphData loadGraphWith) {
        if (loadGraphWith != null) {
            switch (loadGraphWith) {
                case MODERN:
                    // Folder federating both backends: integration/modern/{jdbc,elastic}.json
                    return "/configuration/integration/modern";
//                case CREW: return CrewConfiguration;
//                case GRATEFUL: return GratefulConfiguration;
            }
        }
        // Default (non-MODERN) graph data: elastic-only basic config folder.
        return "/configuration/integration/default";
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
