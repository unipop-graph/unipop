package test;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.test.UnipopGraphProvider;

import java.net.URL;
import java.util.Map;

public class ElasticGraphProvider extends UnipopGraphProvider {

    private static final Class<?> SERVER;

    static {
        try {
            SERVER = Class.forName("org.unipop.elastic.suite.EmbeddedElasticsearchServer");
            SERVER.getMethod("ensureStarted").invoke(null);
        } catch (final ReflectiveOperationException e) {
            throw new IllegalStateException("Could not start Testcontainers Elasticsearch for tests", e);
        }
    }

    public ElasticGraphProvider() throws Exception {
        //patch for failing IO tests that write to disk
        System.setProperty("build.dir", System.getProperty("user.dir") + "/build");
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
        baseConfiguration.put("addresses", getHttpAddress());

        return baseConfiguration;
    }

    private static String getHttpAddress() {
        try {
            return (String) SERVER.getMethod("getHttpAddress").invoke(null);
        } catch (final ReflectiveOperationException e) {
            throw new IllegalStateException("Could not get Elasticsearch HTTP address", e);
        }
    }

    @Override
    public void loadGraphData(Graph graph, LoadGraphWith loadGraphWith, Class testClass, String testName) {
        super.loadGraphData(graph, loadGraphWith, testClass, testName);
        try {
            Object client = SERVER.getMethod("getClient").invoke(null);
            // indices().refresh() — refresh all indices via the ES 8 Java client
            Object indicesClient = client.getClass().getMethod("indices").invoke(client);
            indicesClient.getClass().getMethod("refresh").invoke(indicesClient);
        } catch (final ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to refresh Elasticsearch indices", e);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to refresh Elasticsearch indices", e);
        }
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
        try {
            SERVER.getMethod("deleteAllIndices").invoke(null);
        } catch (final ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to delete Elasticsearch indices", e);
        }
    }

    @Override
    public Object convertId(Object id, Class<? extends Element> c) {
        return id.toString();
    }
}
