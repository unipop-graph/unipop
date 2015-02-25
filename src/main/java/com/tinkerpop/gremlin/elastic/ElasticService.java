package com.tinkerpop.gremlin.elastic;

import com.tinkerpop.gremlin.elastic.structure.ElasticEdge;
import com.tinkerpop.gremlin.elastic.structure.ElasticElement;
import com.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import com.tinkerpop.gremlin.elastic.structure.ElasticVertex;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import jline.internal.Nullable;
import org.apache.commons.lang3.ArrayUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ElasticService {
    private ElasticGraph graph;
    private String indexName;
    private boolean refresh;
    public Client client;
    Node node;

    private static String TYPE = "ty";

    public ElasticService(ElasticGraph graph, String clusterName, String indexName, boolean isLocal, boolean refresh) {
        this.graph = graph;
        this.indexName = indexName;
        this.refresh = refresh;

        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
        node = NodeBuilder.nodeBuilder().local(isLocal).client(!isLocal).settings(settings).build();
        client = node.client();
        node.start();

        IndicesExistsRequest request = new IndicesExistsRequest(indexName);
        IndicesExistsResponse response = client.admin().indices().exists(request).actionGet();
        if (!response.isExists()) {
            settings = ImmutableSettings.settingsBuilder().put("index.analysis.analyzer.default.type", "keyword").build();
            CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(indexName).setSettings(settings);
            CreateIndexResponse createResponse = client.admin().indices().create(createIndexRequestBuilder.request()).actionGet();
        }

        final ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest(indexName).timeout(TimeValue.timeValueSeconds(10)).waitForYellowStatus();
        final ClusterHealthResponse clusterHealth = client.admin().cluster().health(clusterHealthRequest).actionGet();
        if (clusterHealth.isTimedOut()) {
            System.out.print(clusterHealth.getStatus());
        }
    }

    @Override
    public String toString() {
        return "ElasticService{" +
                "indexName='" + indexName + '\'' +
                ", client=" + client +
                ", node=" + node +
                '}';
    }

    public void close() {
        node.close();
    }

    public IndexResponse addElement(String label, @Nullable Object idValue, ElasticElement.Type type, Object... keyValues) {
        IndexRequestBuilder indexRequestBuilder;
        if (idValue == null) indexRequestBuilder = client.prepareIndex(indexName, label).setRefresh(refresh);
        else indexRequestBuilder = client.prepareIndex(indexName, label, idValue.toString()).setRefresh(refresh);
        Object[] all = ArrayUtils.addAll(keyValues, TYPE, type.toString());
        return indexRequestBuilder.setSource(all).execute().actionGet();
    }

    public void deleteElement(Element element) {
        client.prepareDelete(indexName, element.label(), element.id().toString()).setRefresh(refresh).execute().actionGet();
    }

    public void deleteElements(Iterator<Element> elements) {
        elements.forEachRemaining((element) -> deleteElement(element));
    }

    public <V> void addProperty(Element element, String key, V value) {
        client.prepareUpdate(indexName, element.label(), element.id().toString()).setRefresh(refresh).setScript("ctx._source." + key + " = \"" + value + "\"", ScriptService.ScriptType.INLINE).get();
    }

    public void removeProperty(Element element, String key) {
        client.prepareUpdate(indexName, element.label(), element.id().toString()).setRefresh(refresh).setScript("ctx._source.remove(\"" + key + "\")", ScriptService.ScriptType.INLINE).get();
    }

    public void clearAllData() {
        client.prepareDeleteByQuery(indexName).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
    }

    public Iterator<Vertex> searchVertices(FilterBuilder filter, String... labels) {
        Stream<SearchHit> hits = search(filter, ElasticElement.Type.vertex, labels);
        return hits.map((hit) -> createVertex(hit)).iterator();
    }

    private Vertex createVertex(SearchHit hit) {
        ElasticVertex vertex = new ElasticVertex(hit.getId(), hit.getType(), graph);
        hit.getSource().entrySet().forEach((field) -> vertex.addPropertyLocal(field.getKey(), field.getValue()));
        return vertex;
    }

    public Iterator<Edge> searchEdges(FilterBuilder filter, String... labels) {
        Stream<SearchHit> hits = search(filter, ElasticElement.Type.edge, labels);
        return hits.map((hit) -> createEdge(hit)).iterator();
    }

    private Edge createEdge(SearchHit hit) {
        ElasticEdge edge = new ElasticEdge(hit.getId(), hit.getType(), hit.getSource().get(ElasticEdge.InId), hit.getSource().get(ElasticEdge.OutId), graph);
        hit.getSource().entrySet().forEach((field) -> edge.addPropertyLocal(field.getKey(), field.getValue()));
        return edge;
    }

    private Stream<SearchHit> search(FilterBuilder filter, ElasticElement.Type type, String... labels) {
        FilterBuilder finalFilter = FilterBuilders.termFilter(TYPE, type.toString());
        if(filter != null)
            finalFilter = FilterBuilders.andFilter(finalFilter, filter);
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName).setPostFilter(finalFilter).setFrom(0).setSize(100000); //TODO: retrive with scroll for efficiency
        if (labels != null && labels.length > 0 && labels[0] != null) searchRequestBuilder = searchRequestBuilder.setTypes(labels);

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        Iterable<SearchHit> hitsIterable = () -> searchResponse.getHits().iterator();
        Stream<SearchHit> hitStream = StreamSupport.stream(hitsIterable.spliterator(), false);
        return hitStream;
    }
}
