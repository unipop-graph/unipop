package org.apache.tinkerpop.gremlin.elastic;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import org.apache.tinkerpop.gremlin.elastic.structure.*;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.elasticsearch.action.admin.cluster.health.*;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
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
        Settings settings = NodeBuilder.nodeBuilder().settings().put("script.groovy.sandbox.enabled", true).put("script.disable_dynamic", false).build();
        node = NodeBuilder.nodeBuilder().settings(settings).client(false).clusterName(CLUSTER_NAME).build();
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
            put("elasticsearch.index.name",graphName.toLowerCase());
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

            GetMappingsResponse getMappingsResponse = client.admin().indices().prepareGetMappings(indexName).execute().actionGet();
            ArrayList<String> mappings = new ArrayList();
            getMappingsResponse.getMappings().forEach(map -> {
                map.value.forEach(map2 -> mappings.add(map2.value.type()));
            });

            if(mappings.size() > 0) {
                DeleteMappingResponse deleteMappingResponse = client.admin().indices().prepareDeleteMapping(indexName).setType(mappings.toArray(new String[mappings.size()])).execute().actionGet();
            }
            g.close();
        }
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATION;
    }

    @Override
    public Object convertId(Object id) {
        return id.toString();
    }
}
