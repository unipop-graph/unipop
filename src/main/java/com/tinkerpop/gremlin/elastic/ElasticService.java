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
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;

import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ElasticService {
    public static String TYPE = "ty";


    public enum ClientType {
        TRANSPORT_CLIENT,
        NODE_CLIENT,
        NODE
    }

    private ElasticGraph graph;
    private String indexName;
    private boolean refresh;
    public Client client;
    private Node node;

    HashMap<String, Timer> timers = new HashMap<>();

    private Timer timer(String name){
        if(timers.containsKey(name))
            return timers.get(name);
        Timer timer = new Timer(name);
        timers.put(name, timer);
        return timer;
    }

    public static ElasticService create(ElasticGraph graph, Configuration configuration) {
        return new ElasticService(graph,
                configuration.getString("elasticsearch.cluster.name", "elasticsearch"),
                configuration.getString("elasticsearch.index.name", "graph"),
                configuration.getBoolean("elasticsearch.refresh", true),
                configuration.getString("elasticsearch.client", ClientType.NODE.toString()));
    }

    public ElasticService(ElasticGraph graph, String clusterName, String indexName, boolean refresh, String clientType) {
        timer("initialization").start();

        this.graph = graph;
        this.indexName = indexName;
        this.refresh = refresh;
        if(clientType.equals(ClientType.TRANSPORT_CLIENT.toString()))
            createTransportClient(clusterName);
        else if(clientType.equals(ClientType.NODE_CLIENT.toString()))
            createNode(clusterName, true);
        else if(clientType.equals(ClientType.NODE.toString()))
            createNode(clusterName, false);
        else throw new IllegalArgumentException("clientType unknown:" + clientType);

        createIndex(indexName, client);

        timer("initialization").stop();
    }

    private void createTransportClient(String clusterName) {
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
        TransportClient transportClient = new TransportClient(settings);
        transportClient.addTransportAddress(new InetSocketTransportAddress("127.0.0.1", 9300));
        this.client =  transportClient;
    }

    private void createNode(String clusterName, boolean client) {
        this.node = NodeBuilder.nodeBuilder().client(client).clusterName(clusterName).build();
        node.start();
        this.client = node.client();
    }

    private static void createIndex(String indexName, Client client) {
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
    }

    @Override
    public String toString() {
        return "ElasticService{" +
                "indexName='" + indexName + '\'' +
                ", client=" + client +
                '}';
    }

    public void close() {
        client.close();
        if(node != null)
            node.close();
    }

    public IndexResponse addElement(String label, @Nullable Object idValue, ElasticElement.Type type, Object... keyValues) {
        timer("add element").start();
        for (int i = 0; i < keyValues.length; i = i + 2) {
            String key = keyValues[i].toString();
            Object value = keyValues[i + 1];
            ElementHelper.validateProperty(key, value);
        }

        IndexRequestBuilder indexRequestBuilder;
        if (idValue == null) indexRequestBuilder = client.prepareIndex(indexName, label);
        else indexRequestBuilder = client.prepareIndex(indexName, label, idValue.toString());
        Object[] all = ArrayUtils.addAll(keyValues, TYPE, type.toString());
        IndexResponse indexResponse = indexRequestBuilder.setCreate(true).setSource(all).execute().actionGet();
        timer("add element").stop();
        return indexResponse;
    }

    public void deleteElement(Element element) {
        timer("remove element").start();
        client.prepareDelete(indexName, element.label(), element.id().toString()).execute().actionGet();
        timer("remove element").stop();
    }

    public void deleteElements(Iterator<Element> elements) {
        elements.forEachRemaining((element) -> deleteElement(element));
    }

    public <V> void addProperty(Element element, String key, V value) {
        timer("update property").start();
        String stringValue = value.toString();
        if(value instanceof String)
            stringValue = "\"" + value + "\"";

        client.prepareUpdate(indexName, element.label(), element.id().toString())
            .setScript("ctx._source." + key + " = " + stringValue, ScriptService.ScriptType.INLINE).get();
           //.setDoc(key, value).execute().actionGet();
        timer("update property").stop();
    }

    public void removeProperty(Element element, String key) {
        timer("remove property").start();
        client.prepareUpdate(indexName, element.label(), element.id().toString())
                .setScript("ctx._source.remove(\"" + key + "\")", ScriptService.ScriptType.INLINE).get();
        timer("remove property").stop();
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
        timer("get").start();
        MultiGetRequest request = new MultiGetRequest();
        for(int i =0; i < ids.length; i++)
            request.add(indexName, null,ids[i].toString());
        MultiGetResponse multiGetItemResponses = client.multiGet(request).actionGet();
        timer("get").stop();
        return multiGetItemResponses;
    }

    private Stream<SearchHit> search(FilterBuilder filter, ElasticElement.Type type, String... labels) {
        timer("search").start();

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
        timer("search").stop();
        return hitStream;
    }

    public void clearAllData() {
        timer("clear graph").start();
        //client.admin().indices().prepareDelete(indexName).execute().actionGet();
        client.prepareDeleteByQuery(indexName).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        timer("clear graph").stop();
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

    public void collectData() {
        timers.values().forEach((timer)->timer.PrintStats());
    }

    private class Timer {

        private String name;
        StopWatch sw = new StopWatch();
        float numOfRuns = 0;
        float longestRun = 0;
        private long lastRun = 0;

        public Timer(String name) {
            this.name = name;
            sw.reset();
        }

        public void start() {
            if(sw.isSuspended())
                sw.resume();
            else sw.start();
            numOfRuns++;
        }

        public void stop() {
            sw.suspend();
            long time = sw.getTime() - lastRun;
            if(time > longestRun)
                longestRun = time;
            lastRun = time;
        }

        public void PrintStats() {
            if(numOfRuns > 0) {
                float time = sw.getTime() / 1000f;

                System.out.println(name + ": " + twoDForm.format(time) + " total secs, "  + twoDForm.format(time / numOfRuns) + " secs per run, " + numOfRuns + " runs, " + longestRun / 1000f + " sec for longest run")
                ;
            }
        }

    }
    static DecimalFormat twoDForm = new DecimalFormat("#.##");
}
