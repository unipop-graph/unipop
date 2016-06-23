package org.unipop.elastic;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.elasticsearch.client.Client;
import org.unipop.common.test.UnipopGraphProvider;
import org.unipop.elastic.common.ElasticNode;

import java.io.File;
import java.net.URL;
import java.util.Map;

public class ElasticGraphProvider extends UnipopGraphProvider {

    private static String CLUSTER_NAME = "unipop";
    private static String BasicConfiguration = "basic.json";
    private static String InnerEdgeConfiguration = "innerEdge.json";
    private final File dataPath;
    private ElasticNode node;

    public ElasticGraphProvider() throws Exception{
        //patch for failing IO tests that write to disk
        System.setProperty("build.dir", System.getProperty("user.dir") + "\\build");

        String path = new java.io.File( "." ).getCanonicalPath() + "\\data";
        this.dataPath = new File(path);
        FileUtils.deleteQuietly(dataPath);

        this.node = new ElasticNode(dataPath, CLUSTER_NAME);
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        Map<String, Object> baseConfiguration = super.getBaseConfiguration(graphName, test, testMethodName, loadGraphWith);
//        String configurationFile = loadGraphWith != null && loadGraphWith.equals(LoadGraphWith.GraphData.MODERN) ? InnerEdgeConfiguration : BasicConfiguration;
        String configurationFile = BasicConfiguration;
        URL url = this.getClass().getResource("/configuration/" + configurationFile);
        baseConfiguration.put("providers", new String[]{url.getFile()});
        return baseConfiguration;
    }

    @Override
    public void clear(Graph g, Configuration configuration) throws Exception {
        if(node != null) node.deleteIndices();
        super.clear(g, configuration);
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
