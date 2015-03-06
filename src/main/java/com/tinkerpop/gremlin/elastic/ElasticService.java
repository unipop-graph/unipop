package com.tinkerpop.gremlin.elastic;

import com.tinkerpop.gremlin.elastic.structure.ElasticEdge;
import com.tinkerpop.gremlin.elastic.structure.ElasticElement;
import com.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import com.tinkerpop.gremlin.elastic.structure.ElasticVertex;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import jline.internal.Nullable;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ElasticService {
    private ElasticGraph graph;
    private String indexName;
    private boolean refresh;
    public Client client;
    Node node;

    public static String TYPE = "ty";

    public ElasticService(ElasticGraph graph, String clusterName, String indexName, boolean isLocal, boolean refresh, boolean isClient) {

        this.graph = graph;
        this.indexName = indexName;
        this.refresh = refresh;


        this.node = createNode(clusterName, indexName, isLocal, isClient);
        this.client = node.client();
    }

    public static ElasticService create(ElasticGraph graph, Configuration configuration) {
        return new ElasticService(graph,
                configuration.getString("elasticsearch.cluster.name", "elasticsearch"),
                configuration.getString("elasticsearch.index.name", "graph"),
                configuration.getBoolean("elasticsearch.local", true),
                configuration.getBoolean("elasticsearch.refresh", true),
                configuration.getBoolean("elasticsearch.client", true));
    }

    public static Node createNode(String clusterName, String indexName, boolean isLocal, boolean isClient) {
        StopWatch initializationSW = new StopWatch();
        initializationSW.start();

        Node node = NodeBuilder.nodeBuilder().local(isLocal).client(isClient).clusterName(clusterName).build();
        Client client = node.client();
        node.start();

        IndicesExistsRequest request = new IndicesExistsRequest(indexName);
        IndicesExistsResponse response = client.admin().indices().exists(request).actionGet();
        if (!response.isExists()) {
            Settings settings = ImmutableSettings.settingsBuilder().put("index.analysis.analyzer.default.type", "keyword").build();
            CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(indexName).setSettings(settings);
            CreateIndexResponse createResponse = client.admin().indices().create(createIndexRequestBuilder.request()).actionGet();
        }

        final ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest(indexName).timeout(TimeValue.timeValueSeconds(10)).waitForYellowStatus();
        final ClusterHealthResponse clusterHealth = client.admin().cluster().health(clusterHealthRequest).actionGet();
        if (clusterHealth.isTimedOut()) {
            System.out.print(clusterHealth.getStatus());
        }

        initializationSW.stop();
        System.out.println("graph initiazlied in " + initializationSW.getTime()/1000f);
        return node;
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
        for (int i = 0; i < keyValues.length; i = i + 2) {
            String key = keyValues[i].toString();
            Object value = keyValues[i + 1];
            ElementHelper.validateProperty(key, value);
        }

        IndexRequestBuilder indexRequestBuilder;
        if (idValue == null) indexRequestBuilder = client.prepareIndex(indexName, label);
        else indexRequestBuilder = client.prepareIndex(indexName, label, idValue.toString());
        Object[] all = ArrayUtils.addAll(keyValues, TYPE, type.toString());
        return indexRequestBuilder.setCreate(true).setSource(all).execute().actionGet();
    }

    public void deleteElement(Element element) {
        client.prepareDelete(indexName, element.label(), element.id().toString()).execute().actionGet();
    }

    public void deleteElements(Iterator<Element> elements) {
        elements.forEachRemaining((element) -> deleteElement(element));
    }

    public <V> void addProperty(Element element, String key, V value) {
        String stringValue = value.toString();
        if(value instanceof String)
            stringValue = "\"" + value + "\"";

        client.prepareUpdate(indexName, element.label(), element.id().toString())
            .setScript("ctx._source." + key + " = " + stringValue, ScriptService.ScriptType.INLINE).get();
           //.setDoc(key, value).execute().actionGet();
    }

    public void removeProperty(Element element, String key) {
        client.prepareUpdate(indexName, element.label(), element.id().toString())
                .setScript("ctx._source.remove(\"" + key + "\")", ScriptService.ScriptType.INLINE).get();
    }

    public Iterator<Vertex> getVertices(Object... ids){
        if(ids == null || ids.length == 0)
            return Collections.emptyIterator();

        MultiGetResponse responses = get(ids);
        ArrayList<Vertex> vertices = new ArrayList<>(ids.length);
        for(MultiGetItemResponse getResponse : responses){
            GetResponse response = getResponse.getResponse();
            if(!response.isExists())
                throw Graph.Exceptions.elementNotFound(Vertex.class, response.getId());
            vertices.add(createVertex(response.getId(), response.getType(), response.getSource()));
        }
        return vertices.iterator();
    }

    public Iterator<Edge> getEdges(Object... ids){
        if(ids == null || ids.length == 0)
            return Collections.emptyIterator();

        MultiGetResponse responses = get(ids);
        ArrayList<Edge> edges = new ArrayList<>(ids.length);
        for(MultiGetItemResponse getResponse : responses){
            GetResponse response = getResponse.getResponse();
            if(!response.isExists())
                throw Graph.Exceptions.elementNotFound(Edge.class, response.getId());
            edges.add(createEdge(response.getId(), response.getType(), response.getSource()));
        }
        return edges.iterator();
    }

    public Iterator<Vertex> searchVertices(FilterBuilder filter, String... labels) {
        Stream<SearchHit> hits = search(filter, ElasticElement.Type.vertex, labels);
        return hits.map((hit) -> createVertex(hit.getId(), hit.getType(), hit.getSource())).iterator();
    }

    public Iterator<Edge> searchEdges(FilterBuilder filter, String... labels) {
        Stream<SearchHit> hits = search(filter, ElasticElement.Type.edge, labels);
        return hits.map((hit) -> createEdge(hit.getId(), hit.getType(), hit.getSource())).iterator();
    }

    private MultiGetResponse get(Object... ids){
        MultiGetRequest request = new MultiGetRequest();
        for(int i =0; i < ids.length; i++)
            request.add(indexName, null,ids[i].toString());
        return client.multiGet(request).actionGet();
    }

    private Stream<SearchHit> search(FilterBuilder filter, ElasticElement.Type type, String... labels) {
        if(refresh)
            client.admin().indices().prepareRefresh(indexName).execute().actionGet();

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

    public void clearAllData() {
        StopWatch sw = new StopWatch();
        sw.start();
        client.prepareDeleteByQuery(indexName).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        sw.stop();
        System.out.println("clearGraph: " + sw.getTime() / 1000f);
    }

    private Vertex createVertex(Object id, String label, Map<String, Object> fields) {
        ElasticVertex vertex = new ElasticVertex(id, label, null , graph);
        fields.entrySet().forEach((field) -> vertex.addPropertyLocal(field.getKey(), field.getValue()));
        return vertex;
    }

    private Edge createEdge(Object id, String label, Map<String, Object> fields) {
        ElasticEdge edge = new ElasticEdge(id, label, fields.get(ElasticEdge.OutId), fields.get(ElasticEdge.InId), null, graph);
        fields.entrySet().forEach((field) -> edge.addPropertyLocal(field.getKey(), field.getValue()));
        return edge;
    }
}
