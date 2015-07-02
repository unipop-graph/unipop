package org.elasticgremlin;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.elasticgremlin.elasticsearch.ElasticClientFactory;
import org.elasticgremlin.testimpl.ModernStarGraph;
import org.elasticgremlin.structure.*;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class ModernStarGraphGraphProvider extends AbstractGraphProvider {
    private static String CLUSTER_NAME = "test";

    private static final Set<Class> IMPLEMENTATION = new HashSet<Class>() {{
        add(BaseEdge.class);
        add(BaseElement.class);
        add(ModernStarGraph.class);
        add(BaseProperty.class);
        add(BaseVertex.class);
        add(BaseVertexProperty.class);
    }};

    Node node;
    Client client;

    public ModernStarGraphGraphProvider() throws IOException {
        System.setProperty("build.dir", System.getProperty("user.dir") + "\\build");
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
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        System.out.println("graphName: " + graphName);
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, ElasticGraph.class.getName());
            put("elasticsearch.cluster.name", CLUSTER_NAME);
            put("elasticsearch.index.name",graphName.toLowerCase());
            put("elasticsearch.refresh", true);
            put("elasticsearch.client", ElasticClientFactory.ClientType.TRANSPORT_CLIENT.toString());
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
    public Object convertId(Object id, Class<? extends Element> c) {
        return id.toString();
    }
}
