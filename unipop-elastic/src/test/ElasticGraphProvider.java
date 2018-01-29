package test;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.elasticsearch.client.Client;
import org.unipop.test.UnipopGraphProvider;

import java.io.File;
import java.net.URL;
import java.util.Map;

public class ElasticGraphProvider extends UnipopGraphProvider {

    private final File dataPath;
    private LocalNode node;

    public ElasticGraphProvider() throws Exception {
        //patch for failing IO tests that write to disk
        System.setProperty("build.dir", System.getProperty("user.dir") + "/build");

        String path = new java.io.File(".").getCanonicalPath() + "/data";
        this.dataPath = new File(path);
        this.node = new LocalNode(dataPath);
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        Map<String, Object> baseConfiguration = super.getBaseConfiguration(graphName, test, testMethodName, loadGraphWith);
        String configurationFile = getSchemaConfiguration(loadGraphWith);
        URL url = this.getClass().getResource(configurationFile);
        String file = url.getFile();
        if (System.getProperty("os.name").toLowerCase().contains("windows"))
            file = file.substring(1);
        baseConfiguration.put("providers", new String[]{file});
        baseConfiguration.put("bulk.max", 1000);
        baseConfiguration.put("bulk.graph", 10);
        baseConfiguration.put("bulk.multiplier", 10);

        return baseConfiguration;
    }

    @Override
    public void loadGraphData(Graph graph, LoadGraphWith loadGraphWith, Class testClass, String testName) {
        super.loadGraphData(graph, loadGraphWith, testClass, testName);
        node.getClient().admin().indices().prepareRefresh().get();
    }

    public String getSchemaConfiguration(LoadGraphWith.GraphData loadGraphWith) {
        String confDirectory = "/configuration/" + System.getenv("conf") + "/";
        if (loadGraphWith != null)
            switch (loadGraphWith) {
                case MODERN:
                    return confDirectory + "modern";
                case GRATEFUL:
                    return confDirectory + "grateful";
                default:
                    return "/configuration/basic/default";
            }
        return "/configuration/basic/default/";
    }

    @Override
    public void clear(Graph g, Configuration configuration) throws Exception {
        super.clear(g, configuration);
        if (node != null) {
            node.deleteIndices();
        }
    }

    @Override
    public Object convertId(Object id, Class<? extends Element> c) {
        return id.toString();
    }

    public Client getClient() {
        return node.getClient();
    }
}
