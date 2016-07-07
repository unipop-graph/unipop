package org.unipop.elastic;

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

    private static String BasicConfiguration = "basic.json";
    private static String InnerEdgeConfiguration = "innerEdge.json";
    private static String NestedEdgeConfiguration = "nestedEdge.json";
    private static String NestedRefEdgeConfiguration = "nestedRefEdge.json";
    private final File dataPath;
    private LocalNode node;

    public ElasticGraphProvider() throws Exception{
        //patch for failing IO tests that write to disk
        System.setProperty("build.dir", System.getProperty("user.dir") + "\\build");

        String path = new java.io.File( "." ).getCanonicalPath() + "\\data";
        this.dataPath = new File(path);
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        if(this.node == null) this.node = new LocalNode(dataPath);
        Map<String, Object> baseConfiguration = super.getBaseConfiguration(graphName, test, testMethodName, loadGraphWith);
        String configurationFile = getSchemaConfiguration(loadGraphWith);
        URL url = this.getClass().getResource("/configuration/" + configurationFile);
        baseConfiguration.put("providers", new String[]{url.getFile()});
        baseConfiguration.put("bulk.max", 1000);
        baseConfiguration.put("bulk.start", 10);
        baseConfiguration.put("bulk.multiplier", 10);

        return baseConfiguration;
    }

    public String getSchemaConfiguration(LoadGraphWith.GraphData loadGraphWith) {
         if(loadGraphWith != null && loadGraphWith.equals(LoadGraphWith.GraphData.MODERN)) {
//            return InnerEdgeConfiguration;
//             return NestedEdgeConfiguration;
//             return NestedRefEdgeConfiguration;
         }
         return BasicConfiguration;
    }

    @Override
    public void clear(Graph g, Configuration configuration) throws Exception {
        super.clear(g, configuration);
        if(node != null) {
            node.deleteIndices();
        }
    }

    @Override
    public Graph openTestGraph(Configuration config) {
        return super.openTestGraph(config);
    }

    @Override
    public Object convertId(Object id, Class<? extends Element> c) {
        return id.toString();
    }

    public Client getClient() {
        return node.getClient();
    }
}
