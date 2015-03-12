package com.tinkerpop.gremlin.elastic;

import com.tinkerpop.gremlin.AbstractGraphProvider;
import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import com.tinkerpop.gremlin.elastic.structure.*;
import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.admin.cluster.health.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.*;

import java.io.*;
import java.util.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ElasticGraphGraphProvider extends AbstractGraphProvider {

    private static String CLUSTER_NAME = "test";

    private static final Set<Class> IMPLEMENTATION = new HashSet<Class>() {{
        add(ElasticEdge.class);
        add(ElasticElement.class);
        add(ElasticGraph.class);
        add(ElasticProperty.class);
        add(ElasticVertex.class);
        add(ElasticVertexProperty.class);
    }};

    Node node;
    Client client;

    public ElasticGraphGraphProvider() throws IOException {
        String path = new java.io.File( "." ).getCanonicalPath() + "\\data";
        File file = new File(path);
        FileUtils.deleteQuietly(file);

        node = NodeBuilder.nodeBuilder().client(false).clusterName(CLUSTER_NAME).build();
        node.start();
        client = node.client();
        final ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest().timeout(TimeValue.timeValueSeconds(10)).waitForYellowStatus();
        final ClusterHealthResponse clusterHealth = client.admin().cluster().health(clusterHealthRequest).actionGet();
        if (clusterHealth.isTimedOut()) {
            System.out.print(clusterHealth.getStatus());
        }
    }


    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName) {
        System.out.println("graphName: " + graphName);
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, ElasticGraph.class.getName());
            put("elasticsearch.cluster.name", CLUSTER_NAME);
            put("elasticsearch.index.name",graphName);
            put("elasticsearch.refresh", true);
            put("elasticsearch.client", ElasticService.ClientType.TRANSPORT_CLIENT.toString());
        }};
    }

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        if (g != null) {
            //don't use elasticGraph.elasticService.clearAllData(), because sometimes the graph is closed before clear
            String indexName = configuration.getString("elasticsearch.index.name");
            client.prepareDeleteByQuery(indexName).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
            g.close();
        }
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATION;
    }
}
