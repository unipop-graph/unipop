package com.tinkerpop.gremlin.elastic.elasticservice;

import com.eaio.uuid.UUID;
import com.tinkerpop.gremlin.elastic.elasticservice.TimingAccessor.Timer;
import com.tinkerpop.gremlin.elastic.structure.*;
import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.commons.configuration.Configuration;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.update.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.*;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.*;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.*;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.*;

public class ElasticService {
    //region members

    public static class ClientType {
        public static String TRANSPORT_CLIENT = "TRANSPORT_CLIENT";
        public static String NODE_CLIENT = "NODE_CLIENT";
        public static String NODE = "NODE";
    }

    public enum ElementType {
        vertex,
        edge
    }

    private static int DEFAULT_MAX_RESULT_LIMIT = 2000000;
    private static final int MAX_LAZY_GET = 1000;

    private ElasticGraph graph;
    public SchemaProvider schemaProvider;
    private boolean refresh;
    BulkRequestBuilder bulkRequest;
    public Client client;
    private Node node;
    private final boolean upsert;
    TimingAccessor timingAccessor = new TimingAccessor();

    private LazyGetter lazyGetter;


    //endregion

    //region initialization

    public ElasticService(ElasticGraph graph, Configuration configuration) throws IOException {
        timer("initialization").start();

        this.graph = graph;
        this.refresh = configuration.getBoolean("elasticsearch.refresh", true);
        String clusterName = configuration.getString("elasticsearch.cluster.name", "elasticsearch");
        String addresses = configuration.getString("elasticsearch.cluster.address", "127.0.0.1:9300");
        boolean bulk = configuration.getBoolean("elasticsearch.batch", false);
        String schemaProvider = configuration.getString("elasticsearch.schemaProvider", DefaultSchemaProvider.class.getCanonicalName());
        String clientType = configuration.getString("elasticsearch.client", ClientType.NODE);
        this.upsert = configuration.getBoolean("elasticsearch.upsert", false);

        if (clientType.equals(ClientType.TRANSPORT_CLIENT)) createTransportClient(clusterName, addresses);
        else if (clientType.equals(ClientType.NODE_CLIENT)) createNode(clusterName, true);
        else if (clientType.equals(ClientType.NODE)) createNode(clusterName, false);
        else throw new IllegalArgumentException("clientType unknown:" + clientType);

        try {
            Class c = Class.forName(schemaProvider);
            this.schemaProvider = (SchemaProvider) c.newInstance();
            this.schemaProvider.init(this.client, configuration);
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to load SchemaProvider:" + schemaProvider, e);
        }

        if(bulk)
            bulkRequest = client.prepareBulk();

        timer("initialization").stop();
    }

    private void createTransportClient(String clusterName, String addresses) {
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).put("client.transport.sniff", true).build();
        TransportClient transportClient = new TransportClient(settings);
        for(String address : addresses.split(",")) {
            String[] split = address.split(":");
            if(split.length != 2) throw new IllegalArgumentException("Address invalid:" + address +  ". Should contain ip and port, e.g. 127.0.0.1:9300");
            transportClient.addTransportAddress(new InetSocketTransportAddress(split[0], Integer.parseInt(split[1])));
        }
        this.client = transportClient;
    }

    private void createNode(String clusterName, boolean client) {
        this.node = NodeBuilder.nodeBuilder().client(client).clusterName(clusterName).build();
        node.start();
        this.client = node.client();
    }

    public void close() {
        client.close();
        schemaProvider.close();
        if (node != null) node.close();
        timingAccessor.print();
    }

    public void clearAllData() {
        client.prepareDeleteByQuery(schemaProvider.getIndicesForClearGraph())
                .setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
    }

    public BulkResponse commit() {
        if(bulkRequest == null) return null;
        timer("bulk execute").start();
        BulkResponse bulkItemResponses = bulkRequest.execute().actionGet();
        bulkRequest = client.prepareBulk();
        timer("bulk execute").stop();
        return bulkItemResponses;
    }

    //endregion

    //region queries

    public Object addElement(String label, Object idValue, ElementType elementType, Object... keyValues) {
        timer("add element").start();

        for (int i = 0; i < keyValues.length; i = i + 2) {
            String key = keyValues[i].toString();
            Object value = keyValues[i + 1];
            ElementHelper.validateProperty(key, value);
        }
        if(idValue == null) idValue = new UUID().toString();

        SchemaProvider.Result schemaProviderResult = schemaProvider.getIndex(label, idValue, elementType, keyValues);

        if(!upsert) {
            IndexRequest indexRequest = new IndexRequest(schemaProviderResult.getIndex(), label, idValue.toString())
                    .source(keyValues).routing(schemaProviderResult.getRouting()).create(true);
            if(bulkRequest != null) bulkRequest.add(indexRequest);
            else client.index(indexRequest).actionGet();
        }
        else {
            UpdateRequest updateRequest = new UpdateRequest(schemaProviderResult.getIndex(), label, idValue.toString())
                    .doc(keyValues).routing(schemaProviderResult.getRouting()).detectNoop(true);
            updateRequest.docAsUpsert(true);

            if (bulkRequest != null) bulkRequest.add(updateRequest);
            else try {
                client.update(updateRequest).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        timer("add element").stop();
        return idValue;
    }

    public void deleteElement(Element element) {
        timer("remove element").start();
        SchemaProvider.Result schemaProviderResult = schemaProvider.getIndex(element);
        DeleteRequestBuilder deleteRequestBuilder =
                client.prepareDelete(schemaProviderResult.getIndex(), element.label(), element.id().toString())
                        .setRouting(schemaProviderResult.getRouting());
        if(bulkRequest != null) bulkRequest.add(deleteRequestBuilder);
        else deleteRequestBuilder.execute().actionGet();
        timer("remove element").stop();
    }

    public void deleteElements(Iterator<Element> elements) {
        elements.forEachRemaining(this::deleteElement);
    }

    public <V> void addProperty(Element element, String key, V value) {
        timer("update property").start();
        SchemaProvider.Result schemaProviderResult = schemaProvider.getIndex(element);
        UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate(schemaProviderResult.getIndex(), element.label(), element.id().toString()).setDoc(key, value).setRouting(schemaProviderResult.getRouting());
        if(bulkRequest != null) bulkRequest.add(updateRequestBuilder);
        else updateRequestBuilder.execute().actionGet();
        timer("update property").stop();
    }

    public void removeProperty(Element element, String key) {
        timer("remove property").start();
        SchemaProvider.Result schemaProviderResult = schemaProvider.getIndex(element);
        UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate(schemaProviderResult.getIndex(), element.label(), element.id().toString()).setScript("ctx._source.remove(\"" + key + "\")", ScriptService.ScriptType.INLINE).setRouting(schemaProviderResult.getRouting());
        if(bulkRequest != null) bulkRequest.add(updateRequestBuilder);
        else updateRequestBuilder.execute().actionGet();
        timer("remove property").stop();
    }

    public Iterator<Vertex> getVertices(String label,Integer resultsLimit,Object... ids) {
        if (ids == null || ids.length == 0) return Collections.emptyIterator();

        MultiGetResponse responses = get(label, resultsLimit, ids, ElementType.vertex);
        ArrayList<Vertex> vertices = new ArrayList<>(ids.length);
        for (MultiGetItemResponse getResponse : responses) {
            GetResponse response = getResponse.getResponse();
            if(!response.isExists()) continue;
            vertices.add(createVertex(response.getId(), response.getType(), response.getSource()));
        }
        return vertices.iterator();
    }

    public LazyGetter registerLazyVertex(ElasticVertex v) {
        if(lazyGetter == null || lazyGetter.isExecuted() || lazyGetter.multiGetRequest.getItems().size() > MAX_LAZY_GET)
            lazyGetter = new LazyGetter();
        lazyGetter.register(v);
        return lazyGetter;
    }

    public class LazyGetter {
        private boolean executed = false;
        private MultiGetRequest multiGetRequest = new MultiGetRequest();
        private HashMap<String, ElasticVertex> lazyGetters = new HashMap();

        public LazyGetter register(ElasticVertex v) {
            SchemaProvider.Result schemaProviderResult = schemaProvider.getIndex(v.label(), v.id(), ElementType.vertex, null);
            multiGetRequest.add(schemaProviderResult.getIndex(), v.label(), v.id().toString()); //TODO: add routing..?
            lazyGetters.put(v.id().toString(), v);
            return lazyGetter;
        }

        public boolean isExecuted() {
            return executed;
        }

        public void execute() {
            if(!executed) {
                MultiGetResponse multiGetItemResponses = client.multiGet(multiGetRequest).actionGet();
                multiGetItemResponses.forEach(response -> {
                    Map<String, Object> source = response.getResponse().getSource();
                    ElasticVertex v = lazyGetters.get(response.getId());
                    source.entrySet().forEach((field) -> v.addPropertyLocal(field.getKey(), field.getValue()));
                });
                executed = true;
                multiGetRequest = null;
                lazyGetters = null;
            }
        }
    }

    public Iterator<Edge> getEdges(String label,Integer resultsLimit, Object... ids) {
        if (ids == null || ids.length == 0) return Collections.emptyIterator();

        MultiGetResponse responses = get(label, resultsLimit, ids, ElementType.edge);
        ArrayList<Edge> edges = new ArrayList<>(ids.length);
        for (MultiGetItemResponse getResponse : responses) {
            GetResponse response = getResponse.getResponse();
            if (!response.isExists()) throw Graph.Exceptions.elementNotFound(Edge.class, response.getId());
            edges.add(createEdge(response.getId(), response.getType(), response.getSource()));
        }
        return edges.iterator();
    }

    public Iterator<Vertex> searchVertices(BoolFilterBuilder filter, Object[] ids, String[] labels,Integer resultsLimit) {
        if(idsOnlyQuery(filter, ids, labels)) return getVertices(getFirstOrDefaultLabel(labels), resultsLimit, ids);
        FilterBuilder finalFilter = (ids != null && ids.length > 0) ? idsFilter(filter, ids) : filter;
        Stream<SearchHit> hits = search(finalFilter,resultsLimit, ElementType.vertex, labels);
        return hits.map((hit) -> createVertex(hit.getId(), hit.getType(), hit.getSource())).iterator();
    }

    public Iterator<Vertex> searchVertices(BoolFilterBuilder filter, Object[] ids, String[] labels) {
        return searchVertices(filter, ids, labels, null);
    }

    private String getFirstOrDefaultLabel(String[] labels) {
        if(labels == null || labels.length == 0) return null;
        return labels[0];
    }

    private boolean idsOnlyQuery(BoolFilterBuilder filter, Object[] ids, String[] labels) {
        return (filter == null || !filter.hasClauses()) &&(labels == null || labels.length <= 1) && ids != null && ids.length > 0;
    }

    public Iterator<Edge> searchEdges(BoolFilterBuilder filter, Object[] ids, String[] labels,Integer resultsLimit) {
        if(idsOnlyQuery(filter,ids,labels)) getEdges(getFirstOrDefaultLabel(labels),resultsLimit,ids);
        FilterBuilder finalFilter = (ids != null && ids.length > 0) ? idsFilter(filter, ids) : filter;
        Stream<SearchHit> hits = search(finalFilter,resultsLimit, ElementType.edge, labels);
        return hits.map((hit) -> createEdge(hit.getId(), hit.getType(), hit.getSource())).iterator();
    }
    public Iterator<Edge> searchEdges(BoolFilterBuilder filter, Object[] ids, String[] labels){
        return searchEdges(filter, ids, labels, null);
    }


    public static FilterBuilder idsFilter(BoolFilterBuilder boolFilterBuilder, Object[] ids) {
        String[] stringIds = new String[ids.length];
        for(int i = 0; i<ids.length; i++)
            stringIds[i] = ids[i].toString();
        IdsFilterBuilder idsFilterBuilder = FilterBuilders.idsFilter().addIds(stringIds);
        if(boolFilterBuilder.hasClauses())
            return FilterBuilders.andFilter(boolFilterBuilder, idsFilterBuilder);
        return idsFilterBuilder;
    }

    private MultiGetResponse get(String label, Integer resultsLimit, Object[] ids, ElementType type) {
        timer("get").start();
        MultiGetRequest request = new MultiGetRequest();

        int counter = 0;
        while((resultsLimit == null || counter  < resultsLimit) && counter < ids.length){
            Object id = ids[counter];
            SchemaProvider.Result schemaProviderResult = schemaProvider.getIndex(label, id, type, null);
            request.add(schemaProviderResult.getIndex(), label, id.toString()); //TODO: add routing..?
            counter++;
        }

        MultiGetResponse multiGetItemResponses = client.multiGet(request).actionGet();
        timer("get").stop();
        return multiGetItemResponses;
    }

    private Stream<SearchHit> search(FilterBuilder filter,Integer resultsLimit, ElementType elementType, String... labels) {
        timer("search").start();

        BoolFilterBuilder boolFilter;
        if(filter instanceof BoolFilterBuilder) boolFilter = (BoolFilterBuilder) filter;
        else {
            boolFilter = FilterBuilders.boolFilter();
            if(filter != null)
                boolFilter.must(filter);
        }

        if(elementType.equals(ElementType.edge))
            boolFilter.must(FilterBuilders.existsFilter(ElasticEdge.InId));
        else boolFilter.mustNot(FilterBuilders.existsFilter(ElasticEdge.InId));


        SchemaProvider.Result result = schemaProvider.getIndex(boolFilter, elementType, labels);
        if (refresh) client.admin().indices().prepareRefresh(result.getIndex()).execute().actionGet();

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(result.getIndex())
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolFilter)).setRouting(result.getRouting()).setFrom(0);
        //TODO: retrive with scroll for efficiency
        if(resultsLimit != null ) searchRequestBuilder.setSize(resultsLimit);
        else  searchRequestBuilder.setSize(DEFAULT_MAX_RESULT_LIMIT);
        if (labels != null && labels.length > 0 && labels[0] != null)
            searchRequestBuilder = searchRequestBuilder.setTypes(labels);

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        Iterable<SearchHit> hitsIterable = () -> searchResponse.getHits().iterator();
        Stream<SearchHit> hitStream = StreamSupport.stream(hitsIterable.spliterator(), false);
        timer("search").stop();
        return hitStream;
    }


    private Vertex createVertex(Object id, String label, Map<String, Object> fields) {
        ElasticVertex vertex = new ElasticVertex(id, label, null, graph, false);
        fields.entrySet().forEach((field) -> vertex.addPropertyLocal(field.getKey(), field.getValue()));
        return vertex;
    }

    private Edge createEdge(Object id, String label, Map<String, Object> fields) {
        ElasticEdge edge = new ElasticEdge(id, label, fields.get(ElasticEdge.OutId),fields.get(ElasticEdge.OutLabel).toString(), fields.get(ElasticEdge.InId),fields.get(ElasticEdge.InLabel).toString(), null, graph);
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
                "schema='" + schemaProvider + '\'' +
                ", client=" + client +
                '}';
    }
    //endregion
}
