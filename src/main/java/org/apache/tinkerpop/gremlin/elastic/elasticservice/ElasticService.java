package org.apache.tinkerpop.gremlin.elastic.elasticservice;

import com.eaio.uuid.UUID;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.elastic.structure.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.update.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.*;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
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

    private ElasticGraph graph;
    public IndexProvider indexProvider;
    private boolean refresh;
    BulkRequestBuilder bulkRequest;
    public Client client;
    private Node node;
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
        String indexProvider = configuration.getString("elasticsearch.indexProvider", DefaultIndexProvider.class.getCanonicalName());
        String clientType = configuration.getString("elasticsearch.client", ClientType.NODE);

        if (clientType.equals(ClientType.TRANSPORT_CLIENT)) createTransportClient(clusterName, addresses);
        else if (clientType.equals(ClientType.NODE_CLIENT)) createNode(clusterName, true);
        else if (clientType.equals(ClientType.NODE)) createNode(clusterName, false);
        else throw new IllegalArgumentException("clientType unknown:" + clientType);

        try {
            Class c = Class.forName(indexProvider);
            this.indexProvider = (IndexProvider) c.newInstance();
            this.indexProvider.init(this.client, configuration);
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to load indexProvider:" + indexProvider, e);
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
        Settings settings = NodeBuilder.nodeBuilder().settings().put("script.groovy.sandbox.enabled", true).put("script.disable_dynamic", false).build();
        this.node = NodeBuilder.nodeBuilder().client(client).clusterName(clusterName).settings(settings).build();
        node.start();
        this.client = node.client();
    }

    public void close() {
        client.close();
        indexProvider.close();
        if (node != null) node.close();
        timingAccessor.print();
    }

    public void clearAllData() {
        client.prepareDeleteByQuery(indexProvider.getIndicesForClearGraph())
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

    public LazyGetter getLazyGetter() {
        if(lazyGetter == null || !lazyGetter.canRegister())
            lazyGetter = new LazyGetter(this);
        return lazyGetter;
    }

    //endregion

    //region queries

    public Object addElement(Boolean upsert, String label, Object idValue, ElementType elementType, Object... keyValues) {
        timer("add element").start();

        for (int i = 0; i < keyValues.length; i = i + 2) {
            String key = keyValues[i].toString();
            Object value = keyValues[i + 1];
            ElementHelper.validateProperty(key, value);
        }
        if(idValue == null) idValue = new UUID().toString();

        IndexProvider.MutateResult indexProviderResult = indexProvider.getIndex(label, idValue, elementType, keyValues);

        if(!upsert) {
            IndexRequest indexRequest = new IndexRequest(indexProviderResult.getIndex(), label, idValue.toString())
                    .source(keyValues).routing(indexProviderResult.getRouting()).create(true);
            if(bulkRequest != null) bulkRequest.add(indexRequest);
            else client.index(indexRequest).actionGet();
        }
        else {
            UpdateRequest updateRequest = new UpdateRequest(indexProviderResult.getIndex(), label, idValue.toString())
                    .doc(keyValues).routing(indexProviderResult.getRouting()).detectNoop(true);
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
        IndexProvider.MutateResult indexProviderResult = indexProvider.getIndex(element);
        DeleteRequestBuilder deleteRequestBuilder =
                client.prepareDelete(indexProviderResult.getIndex(), element.label(), element.id().toString())
                        .setRouting(indexProviderResult.getRouting());
        if(bulkRequest != null) bulkRequest.add(deleteRequestBuilder);
        else deleteRequestBuilder.execute().actionGet();
        timer("remove element").stop();
    }

    public void deleteElements(Iterator<Element> elements) {
        elements.forEachRemaining(this::deleteElement);
    }

    public <V> void addProperty(Element element, String key, V value) {
        timer("update property").start();
        IndexProvider.MutateResult indexProviderResult = indexProvider.getIndex(element);
        UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate(indexProviderResult.getIndex(), element.label(), element.id().toString()).setDoc(key, value).setRouting(indexProviderResult.getRouting());
        if(bulkRequest != null) bulkRequest.add(updateRequestBuilder);
        else updateRequestBuilder.execute().actionGet();
        timer("update property").stop();
    }

    public void removeProperty(Element element, String key) {
        timer("remove property").start();
        IndexProvider.MutateResult indexProviderResult = indexProvider.getIndex(element);
        UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate(indexProviderResult.getIndex(), element.label(), element.id().toString()).setScript("ctx._source.remove(\"" + key + "\")", ScriptService.ScriptType.INLINE).setRouting(indexProviderResult.getRouting());
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

    private MultiGetResponse get(String label, Integer resultsLimit, Object[] ids, ElementType type) {
        timer("get").start();
        MultiGetRequest request = new MultiGetRequest().refresh(refresh);

        int counter = 0;
        while((resultsLimit == null || counter  < resultsLimit) && counter < ids.length){
            Object id = ids[counter];
            IndexProvider.MutateResult indexProviderResult = indexProvider.getIndex(label, id, type, null);
            request.add(indexProviderResult.getIndex(), label, id.toString()); //TODO: add routing..?
            counter++;
        }

        MultiGetResponse multiGetItemResponses = client.multiGet(request).actionGet();
        timer("get").stop();
        return multiGetItemResponses;
    }

    public Iterator<Vertex> searchVertices(List<HasContainer> hasContainers, Integer resultsLimit) {
        BoolFilterBuilder boolFilter = createFilterBuilder(hasContainers);
        boolFilter.must(FilterBuilders.missingFilter(ElasticEdge.InId));

        Stream<SearchHit> hits = search(hasContainers, boolFilter, resultsLimit, ElementType.vertex);
        return hits.map((hit) -> createVertex(hit.getId(), hit.getType(), hit.getSource())).iterator();
    }

    public Iterator<Edge> searchEdges(List<HasContainer> hasContainers, Integer resultsLimit, Direction direction, Object... vertexIds) {
        BoolFilterBuilder boolFilter = createFilterBuilder(hasContainers);
        boolFilter.must(FilterBuilders.existsFilter(ElasticEdge.InId));

        if(direction != null && vertexIds != null) {
            if (direction == Direction.IN) boolFilter.must(FilterBuilders.termsFilter(ElasticEdge.InId, vertexIds));
            else if (direction == Direction.OUT)
                boolFilter.must(FilterBuilders.termsFilter(ElasticEdge.OutId, vertexIds));
            else if (direction == Direction.BOTH)
                boolFilter.should(FilterBuilders.termsFilter(ElasticEdge.InId, vertexIds), FilterBuilders.termsFilter(ElasticEdge.OutId, vertexIds));
            else throw new EnumConstantNotPresentException(direction.getClass(), direction.name());
        }

        Stream<SearchHit> hits = search(hasContainers, boolFilter, resultsLimit, ElementType.edge);
        return hits.map((hit) -> createEdge(hit.getId(), hit.getType(), hit.getSource())).iterator();
    }

    private Stream<SearchHit> search(List<HasContainer> hasContainers, BoolFilterBuilder boolFilter, Integer resultsLimit, ElementType elementType) {
        timer("search").start();

        IndexProvider.SearchResult result = indexProvider.getIndex(hasContainers, elementType);
        if (refresh) client.admin().indices().prepareRefresh(result.getIndex()).execute().actionGet();

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(result.getIndex())
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolFilter))
                .setRouting(result.getRouting()).setFrom(0);
        //TODO: retrive with scroll for efficiency
        if(resultsLimit != null ) searchRequestBuilder.setSize(resultsLimit);
        else  searchRequestBuilder.setSize(DEFAULT_MAX_RESULT_LIMIT);

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

    private TimingAccessor.Timer timer(String name) {
        return timingAccessor.timer(name);
    }

    public void collectData() {
        timingAccessor.print();
    }

    @Override
    public String toString() {
        return "ElasticService{" +
                "schema='" + indexProvider + '\'' +
                ", client=" + client +
                '}';
    }

    private BoolFilterBuilder createFilterBuilder(List<HasContainer> hasContainers) {
        BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();
        if(hasContainers != null) hasContainers.forEach(has -> addFilter(boolFilter, has));
        return boolFilter;
    }

    private void addFilter(BoolFilterBuilder boolFilterBuilder, HasContainer has){
        if(has.key.equals("~id")) {
            IdsFilterBuilder idsFilterBuilder = FilterBuilders.idsFilter();
            if(has.value.getClass().isArray()) {
                for(Object id : (Object[])has.value)
                    idsFilterBuilder.addIds(id.toString());
            }
            else idsFilterBuilder.addIds(has.value.toString());
            boolFilterBuilder.must(idsFilterBuilder);
        }
        else if(has.key.equals("~label")) {
            if(has.value.getClass().isArray()) {
                Object[] labels = (Object[]) has.value;
                if(labels.length == 1)
                    boolFilterBuilder.must(FilterBuilders.typeFilter(labels[0].toString()));
                else {
                    FilterBuilder[] filters = new FilterBuilder[labels.length];
                    for(int i = 0; i < labels.length; i++)
                        filters[i] = FilterBuilders.typeFilter(labels[i].toString());
                    boolFilterBuilder.must(FilterBuilders.orFilter(filters));
                }
            }
            else boolFilterBuilder.must(FilterBuilders.typeFilter(has.value.toString()));
        }
        else if (has.predicate instanceof Compare) {
            String predicateString = has.predicate.toString();
            switch (predicateString) {
                case ("eq"):
                    boolFilterBuilder.must(FilterBuilders.termFilter(has.key, has.value));
                    break;
                case ("neq"):
                    boolFilterBuilder.mustNot(FilterBuilders.termFilter(has.key, has.value));
                    break;
                case ("gt"):
                    boolFilterBuilder.must(FilterBuilders.rangeFilter(has.key).gt(has.value));
                    break;
                case ("gte"):
                    boolFilterBuilder.must(FilterBuilders.rangeFilter(has.key).gte(has.value));
                    break;
                case ("lt"):
                    boolFilterBuilder.must(FilterBuilders.rangeFilter(has.key).lt(has.value));
                    break;
                case ("lte"):
                    boolFilterBuilder.must(FilterBuilders.rangeFilter(has.key).lte(has.value));
                    break;
                case("inside"):
                    List items =(List) has.value;
                    Object firstItem = items.get(0);
                    Object secondItem = items.get(1);
                    boolFilterBuilder.must(FilterBuilders.rangeFilter(has.key).from(firstItem).to(secondItem));
                    break;
                default:
                    throw new IllegalArgumentException("predicate not supported in has step: " + has.predicate.toString());
            }
        } else if (has.predicate instanceof Contains) {
            if (has.predicate == Contains.without) boolFilterBuilder.must(FilterBuilders.missingFilter(has.key));
            else if (has.predicate == Contains.within){
                if(has.value == null) boolFilterBuilder.must(FilterBuilders.existsFilter(has.key));
                else  boolFilterBuilder.must(FilterBuilders.termsFilter (has.key, has.value));
            }
        } else if (has.predicate instanceof Geo) boolFilterBuilder.must(new GeoShapeFilterBuilder(has.key, GetShapeBuilder(has.value), ((Geo) has.predicate).getRelation()));
        else throw new IllegalArgumentException("predicate not supported by elastic-gremlin: " + has.predicate.toString());
    }

    private ShapeBuilder GetShapeBuilder(Object object) {
        try {
            String geoJson = (String) object;
            XContentParser parser = JsonXContent.jsonXContent.createParser(geoJson);
            parser.nextToken();

            return ShapeBuilder.parse(parser);
        } catch (Exception e) {
            return null;
        }
    }
    //endregion
}
