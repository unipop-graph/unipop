package org.unipop.elastic;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.unipop.common.test.TestInitializer;
import org.unipop.elastic.controller.helpers.ElasticClientFactory;
import org.unipop.elastic.controller.helpers.ElasticHelper;

import java.io.File;

public class ElasticTestInitializer implements TestInitializer {

    private static String CLUSTER_NAME = "test";
    private Client client;

    public ElasticTestInitializer(Client client) throws Exception{
        //patch for failing IO tests that write to disk
        System.setProperty("build.dir", System.getProperty("user.dir") + "\\build");
        //Delete elasticsearch 'data' directory
        String path = new java.io.File( "." ).getCanonicalPath() + "\\data";
        File file = new File(path);
        FileUtils.deleteQuietly(file);

        Node node = ElasticClientFactory.createNode(CLUSTER_NAME, false, 0);
        client = node.client();
    }

    @Override
    public void clear(Graph g, Configuration configuration) {
        String indexName = configuration.getString("graphName");
        if(indexName != null)
            ElasticHelper.clearIndex(client, indexName);
    }
}
