package org.apache.tinkerpop.gremlin.elastic.elasticservice;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.elastic.process.optimize.ElasticOptimizationStrategy;
import org.apache.tinkerpop.gremlin.elastic.structure.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.*;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.*;
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


    private ElasticGraph graph;
    public IndexProvider indexProvider;
    private boolean refresh;
    public Client client;
    private Node node;
    TimingAccessor timingAccessor = new TimingAccessor();
    private LazyGetter lazyGetter;

    //endregion

    //region initialization

    public ElasticService(ElasticGraph graph, Configuration configuration) throws IOException {
        timer("initialization").start();

        this.graph = graph;
        this.refresh = configuration.getBoolean("elasticsearch.refresh", false);
        String clusterName = configuration.getString("elasticsearch.cluster.name", "elasticsearch");
        String addresses = configuration.getString("elasticsearch.cluster.address", "127.0.0.1:9300");
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
        this.node = NodeBuilder.nodeBuilder().client(client).data(!client).clusterName(clusterName).settings(settings).build();
        node.start();
        this.client = node.client();
    }

    public void close() {
        client.close();
        indexProvider.close();
        if (node != null) node.close();
        timingAccessor.print();
    }

    //endregion

    //region mutate

    public org.elasticsearch.action.index.IndexResponse addElement(Element element, boolean create) {
        timer("add element").start();
        IndexResponse indexResponse = client.index(indexRequest(element, create)).actionGet();
        timer("add element").stop();
        return indexResponse;
    }

    public BulkResponse addElements(Iterator<Element> elements, boolean create) {
        timer("add elements").start();
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        elements.forEachRemaining(element -> bulkRequest.add(indexRequest(element, create)));
        BulkResponse bulkItemResponses = bulkRequest.execute().actionGet();
        timer("add elements").stop();
        return bulkItemResponses;
    }

    private IndexRequest indexRequest(Element element, boolean create) {
        IndexProvider.IndexResult indexProviderResult = indexProvider.getIndex(element);
        return new IndexRequest(indexProviderResult.getIndex(), element.label(), element.id().toString())
                .source(propertiesMap(element)).routing(indexProviderResult.getRouting()).create(create);
    }

    private Map propertiesMap(Element element) {
        if(element instanceof ElasticElement)
            return ((ElasticElement)element).allFields();

        Map<String, Object> map = new HashMap<>();
        element.properties().forEachRemaining(property -> map.put(property.key(), property.value()));
        return map;
    }

    public Object upsertElement(Element element) throws ExecutionException, InterruptedException {
        timer("upsert element").start();
        UpdateRequest updateRequest = updateRequest(element, true);
        UpdateResponse updateResponse = client.update(updateRequest).get();
        timer("upsert element").stop();
        return updateResponse;
    }

    public BulkResponse upsertElements(Iterator<Element> elements) {
        timer("upsert elements").start();
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        elements.forEachRemaining(element -> bulkRequest.add(updateRequest(element, true)));
        BulkResponse bulkItemResponses = bulkRequest.execute().actionGet();
        timer("upsert elements").stop();
        return bulkItemResponses;
    }
    private UpdateRequest updateRequest(Element element, boolean upsert) {
        IndexProvider.IndexResult indexProviderResult = indexProvider.getIndex(element);
        UpdateRequest updateRequest = new UpdateRequest(indexProviderResult.getIndex(), element.label(), element.id().toString())
                .doc(propertiesMap(element)).routing(indexProviderResult.getRouting());
        if(upsert)
            updateRequest.detectNoop(true).docAsUpsert(true);
        return updateRequest;
    }


    public DeleteResponse deleteElement(Element element) {
        timer("remove element").start();
        DeleteRequestBuilder deleteRequestBuilder = deleteRequest(element);
        DeleteResponse deleteResponse = deleteRequestBuilder.execute().actionGet();
        timer("remove element").stop();
        return deleteResponse;
    }

    public BulkResponse deleteElements(Iterator<Element> elements) {
        timer("upsert elements").start();
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        elements.forEachRemaining(element -> bulkRequest.add(deleteRequest(element)));
        BulkResponse bulkItemResponses = bulkRequest.execute().actionGet();
        timer("upsert elements").stop();
        return bulkItemResponses;
    }

    private DeleteRequestBuilder deleteRequest(Element element) {
        IndexProvider.IndexResult indexProviderResult = indexProvider.getIndex(element);
        return client.prepareDelete(indexProviderResult.getIndex(), element.label(), element.id().toString())
                        .setRouting(indexProviderResult.getRouting());
    }

    public DeleteByQueryResponse clearAllData() {
        return client.prepareDeleteByQuery(indexProvider.getIndicesForClearGraph())
                .setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
    }

    //endregion

    //region query

    public LazyGetter getLazyGetter() {
        if(lazyGetter == null || !lazyGetter.canRegister())
            lazyGetter = new LazyGetter(this);
        return lazyGetter;
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
            if(id instanceof Element) id = ((Element)id).id();
            IndexProvider.IndexResult indexProviderResult = indexProvider.getIndex(label, id, type);
            request.add(indexProviderResult.getIndex(), label, id.toString()); //TODO: add routing..?
            counter++;
        }

        MultiGetResponse multiGetItemResponses = client.multiGet(request).actionGet();
        timer("get").stop();
        return multiGetItemResponses;
    }

    public Iterator<Vertex> searchVertices(Predicates predicates) {
        BoolFilterBuilder boolFilter = createFilterBuilder(predicates.hasContainers);
        boolFilter.must(FilterBuilders.missingFilter(ElasticEdge.InId));

        Stream<SearchHit> hits = search(predicates, boolFilter, ElementType.vertex);
        return hits.map((hit) -> createVertex(hit.getId(), hit.getType(), hit.getSource())).iterator();
    }

    public Iterator<Edge> searchEdges(Predicates predicates, Direction direction, Object... vertexIds) {
        BoolFilterBuilder boolFilter = createFilterBuilder(predicates.hasContainers);
        boolFilter.must(FilterBuilders.existsFilter(ElasticEdge.InId));

        if(direction != null && vertexIds != null && vertexIds.length > 0) {
            if (direction == Direction.IN) boolFilter.must(FilterBuilders.termsFilter(ElasticEdge.InId, vertexIds));
            else if (direction == Direction.OUT)
                boolFilter.must(FilterBuilders.termsFilter(ElasticEdge.OutId, vertexIds));
            else if (direction == Direction.BOTH)
                boolFilter.should(FilterBuilders.termsFilter(ElasticEdge.InId, vertexIds), FilterBuilders.termsFilter(ElasticEdge.OutId, vertexIds));
            else throw new EnumConstantNotPresentException(direction.getClass(), direction.name());
        }

        Stream<SearchHit> hits = search(predicates, boolFilter, ElementType.edge);
        return hits.map((hit) -> createEdge(hit.getId(), hit.getType(), hit.getSource())).iterator();
    }

    private Stream<SearchHit> search(Predicates predicates, BoolFilterBuilder boolFilter, ElementType elementType) {
        timer("search").start();

        IndexProvider.MultiIndexResult result = indexProvider.getIndex(predicates.hasContainers, elementType);
        if (refresh) client.admin().indices().prepareRefresh(result.getIndex()).execute().actionGet();

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(result.getIndex())
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolFilter))
                .setRouting(result.getRouting()).setFrom((int) predicates.limitLow).setSize((int) predicates.limitHigh);
        //TODO: retrive with scroll for efficiency

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
