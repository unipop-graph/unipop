package com.tinkerpop.gremlin.elastic;

import com.tinkerpop.gremlin.elastic.structure.*;
import com.tinkerpop.gremlin.elastic.tools.TimingAccessor;
import com.tinkerpop.gremlin.elastic.tools.TimingAccessor.Timer;
import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import jline.internal.Nullable;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.ArrayUtils;
import org.elasticsearch.action.admin.cluster.health.*;
import org.elasticsearch.action.admin.indices.create.*;
import org.elasticsearch.action.admin.indices.exists.indices.*;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.*;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.*;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.*;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;

import java.util.*;
import java.util.stream.*;

public class ElasticService {

    //region static

    public static String TYPE = "ty";

    public static class ClientType {
        public static String TRANSPORT_CLIENT = "TRANSPORT_CLIENT";
        public static String NODE_CLIENT = "NODE_CLIENT";
        public static String NODE = "NODE";
    }

    //endregion

    //region members

    private ElasticGraph graph;
    private String indexName;
    private boolean refresh;
    public Client client;
    private Node node;
    TimingAccessor timingAccessor = new TimingAccessor();

    //endregion

    //region initialization

    public static ElasticService create(ElasticGraph graph, Configuration configuration) {
        return new ElasticService(graph,
                configuration.getString("elasticsearch.cluster.name", "elasticsearch"),
                configuration.getString("elasticsearch.index.name", "graph"),
                configuration.getBoolean("elasticsearch.refresh", true),
                configuration.getString("elasticsearch.client", ClientType.NODE));
    }

    public ElasticService(ElasticGraph graph, String clusterName, String indexName, boolean refresh, String clientType) {
        timer("initialization").start();

        this.graph = graph;
        this.indexName = indexName;
        this.refresh = refresh;
        if (clientType.equals(ClientType.TRANSPORT_CLIENT)) createTransportClient(clusterName);
        else if (clientType.equals(ClientType.NODE_CLIENT)) createNode(clusterName, true);
        else if (clientType.equals(ClientType.NODE)) createNode(clusterName, false);
        else throw new IllegalArgumentException("clientType unknown:" + clientType);

        createIndex(indexName, client);

        timer("initialization").stop();
    }

    private void createTransportClient(String clusterName) {
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
        TransportClient transportClient = new TransportClient(settings);
        transportClient.addTransportAddress(new InetSocketTransportAddress("127.0.0.1", 9300));
        this.client = transportClient;
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
            client.admin().indices().create(createIndexRequestBuilder.request()).actionGet();
        }

        final ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest(indexName).timeout(TimeValue.timeValueSeconds(10)).waitForYellowStatus();
        final ClusterHealthResponse clusterHealth = client.admin().cluster().health(clusterHealthRequest).actionGet();
        if (clusterHealth.isTimedOut()) {
            System.out.print(clusterHealth.getStatus());
        }
    }

    public void close() {
        client.close();
        if (node != null) node.close();
    }

    //endregion

    //region queries

    public void clearAllData() {
        timer("clear graph").start();
        client.prepareDeleteByQuery(indexName).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        timer("clear graph").stop();
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
        elements.forEachRemaining(this::deleteElement);
    }

    public <V> void addProperty(Element element, String key, V value) {
        timer("update property").start();
        client.prepareUpdate(indexName, element.label(), element.id().toString())
                .setDoc(key, value).execute().actionGet();
        timer("update property").stop();
    }

    public void removeProperty(Element element, String key) {
        timer("remove property").start();
        client.prepareUpdate(indexName, element.label(), element.id().toString()).setScript("ctx._source.remove(\"" + key + "\")", ScriptService.ScriptType.INLINE).get();
        timer("remove property").stop();
    }

    public Iterator<Vertex> getVertices(Object... ids) {
        if (ids == null || ids.length == 0) return Collections.emptyIterator();

        MultiGetResponse responses = get(ids);
        ArrayList<Vertex> vertices = new ArrayList<>(ids.length);
        for (MultiGetItemResponse getResponse : responses) {
            GetResponse response = getResponse.getResponse();
            if (!response.isExists()) throw Graph.Exceptions.elementNotFound(Vertex.class, response.getId());
            vertices.add(createVertex(response.getId(), response.getType(), response.getSource()));
        }
        return vertices.iterator();
    }

    public Iterator<Edge> getEdges(Object... ids) {
        if (ids == null || ids.length == 0) return Collections.emptyIterator();

        MultiGetResponse responses = get(ids);
        ArrayList<Edge> edges = new ArrayList<>(ids.length);
        for (MultiGetItemResponse getResponse : responses) {
            GetResponse response = getResponse.getResponse();
            if (!response.isExists()) throw Graph.Exceptions.elementNotFound(Edge.class, response.getId());
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

    private MultiGetResponse get(Object... ids) {
        timer("get").start();
        MultiGetRequest request = new MultiGetRequest();
        for (Object id : ids)
            request.add(indexName, null, id.toString());
        MultiGetResponse multiGetItemResponses = client.multiGet(request).actionGet();
        timer("get").stop();
        return multiGetItemResponses;
    }

    private Stream<SearchHit> search(FilterBuilder filter, ElasticElement.Type type, String... labels) {
        timer("search").start();

        if (refresh) client.admin().indices().prepareRefresh(indexName).execute().actionGet();

        FilterBuilder finalFilter = FilterBuilders.termFilter(TYPE, type.toString());
        if (filter != null) finalFilter = FilterBuilders.andFilter(finalFilter, filter);
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName).setPostFilter(finalFilter).setFrom(0).setSize(100000); //TODO: retrive with scroll for efficiency
        if (labels != null && labels.length > 0 && labels[0] != null)
            searchRequestBuilder = searchRequestBuilder.setTypes(labels);

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        Iterable<SearchHit> hitsIterable = () -> searchResponse.getHits().iterator();
        Stream<SearchHit> hitStream = StreamSupport.stream(hitsIterable.spliterator(), false);
        timer("search").stop();
        return hitStream;
    }


    private Vertex createVertex(Object id, String label, Map<String, Object> fields) {
        ElasticVertex vertex = new ElasticVertex(id, label, null, graph);
        fields.entrySet().forEach((field) -> vertex.addPropertyLocal(field.getKey(), field.getValue()));
        return vertex;
    }

    private Edge createEdge(Object id, String label, Map<String, Object> fields) {
        ElasticEdge edge = new ElasticEdge(id, label, fields.get(ElasticEdge.OutId), fields.get(ElasticEdge.InId), null, graph);
        fields.entrySet().forEach((field) -> edge.addPropertyLocal(field.getKey(), field.getValue()));
        return edge;
    }

    //endregion

    //region meta

    private Timer timer(String name) {
        return timingAccessor.timer(name);
    }

    public void collectData() {
        timingAccessor.print();
    }

    @Override
    public String toString() {
        return "ElasticService{" +
                "indexName='" + indexName + '\'' +
                ", client=" + client +
                '}';
    }
    //endregion
}
