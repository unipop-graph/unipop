package org.unipop.elastic.document.schema;

import io.searchbox.action.BulkableAction;
import io.searchbox.core.Delete;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanGlobalStep;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.javatuples.Pair;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.elastic.common.ElasticClient;
import org.unipop.elastic.common.FilterHelper;
import org.unipop.elastic.document.Document;
import org.unipop.elastic.document.DocumentSchema;
import org.unipop.elastic.document.schema.property.IndexPropertySchema;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.query.aggregation.ReduceQuery;
import org.unipop.query.aggregation.ReduceVertexQuery;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.SearchQuery;
import org.unipop.schema.element.AbstractElementSchema;
import org.unipop.schema.property.PropertySchema;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;
import org.unipop.util.PropertySchemaFactory;

import java.io.IOException;
import java.util.*;

public abstract class AbstractDocSchema<E extends Element> extends AbstractElementSchema<E> implements DocumentSchema<E> {
    protected final ElasticClient client;
    protected String type;
    protected IndexPropertySchema index;
    protected ObjectMapper mapper = new ObjectMapper();

    public AbstractDocSchema(JSONObject configuration, ElasticClient client, UniGraph graph) throws JSONException {
        super(configuration, graph);
        this.client = client;
        this.index = (IndexPropertySchema) PropertySchemaFactory.createPropertySchema("index", json.opt("index"), this);
        if (index != null) index.addValidation(client::validateIndex);
        this.type = json.optString("type", null);
    }

    @Override
    public Search getSearch(SearchQuery<E> query) {
        PredicatesHolder predicatesHolder = this.toPredicates(query.getPredicates());
        if (predicatesHolder.getClause().equals(PredicatesHolder.Clause.Abort)) return null;
        QueryBuilder queryBuilder = createQueryBuilder(predicatesHolder);
        return createSearch(query, queryBuilder);
    }

    @Override
    public Search getReduce(ReduceQuery query) {
        PredicatesHolder predicatesHolder = getReducePredicates(query);
        if (predicatesHolder.getClause().equals(PredicatesHolder.Clause.Abort)) return null;
        QueryBuilder queryBuilder = createQueryBuilder(predicatesHolder);
        SearchSourceBuilder searchBuilder = createSearchBuilder(query, queryBuilder);
        createReduce(query, searchBuilder);
        Search.Builder search = new Search.Builder(searchBuilder.toString().replace("\n", ""))
                .addIndex(index.getIndex(query.getPredicates()))
                .ignoreUnavailable(true)
                .allowNoIndices(true);

        if (type != null)
            search.addType(type);

        return search.build();
    }

    @Override
    public Search getLocal(LocalQuery query) {
        PropertySchema idSchema = getPropertySchemas().stream().filter(schema -> schema.getKey().equals(T.id.getAccessor())).findFirst().get();
        Iterator<String> idFields = idSchema.toFields(Collections.emptySet()).iterator();
        AggregationBuilder terms = createTerms("", idFields);
        SearchQuery searchQuery = query.getSearchQuery();
        SearchSourceBuilder searchBuilder = this.createSearchBuilder(searchQuery, createQueryBuilder(searchQuery.getPredicates()));
        searchBuilder.aggregation(terms.subAggregation(AggregationBuilders.topHits("hits").setSize(searchQuery.getLimit())));
        Search.Builder search = new Search.Builder(searchBuilder.toString().replace("\n", ""))
                .addIndex(index.getIndex(searchQuery.getPredicates()))
                .ignoreUnavailable(true)
                .allowNoIndices(true);

        if (type != null)
            search.addType(type);

        return search.build(); // TODO: implement using terms aggregations and filters and etc. from the search query
    }

    protected AggregationBuilder createTerms(String name, Iterator<String> fields){
        int count = 1;
        String next = fields.next();
        if (next.equals("_id")) next = "_uid";
        AggregationBuilder agg = AggregationBuilders.terms(name + "_id_" + count).field(next);
        while (fields.hasNext()){
            next = fields.next();
            if (next.equals("_id")) next = "_uid";
            count++;
            AggregationBuilder sub = AggregationBuilders.terms(name + "_id_" + count).field(next);
            agg.subAggregation(sub);
            agg = sub;
        }
        return agg;
    }

    protected PredicatesHolder getReducePredicates(ReduceQuery query){
        return this.toPredicates(query.getPredicates());
    }

    protected void createReduce(ReduceQuery query, SearchSourceBuilder searchBuilder) {
        switch (query.getOp()) {
            case Count:
                searchBuilder.size(0);
                if (query.getReduceOn() != null)
                    searchBuilder.aggregation(AggregationBuilders.count("count").field(query.getReduceOn()));
                break;
            case Sum:
                searchBuilder.size(0);
                searchBuilder.aggregation(AggregationBuilders.sum("sum").field(query.getReduceOn()));
                break;
            case Max:
                searchBuilder.size(0);
                searchBuilder.aggregation(AggregationBuilders.max("max").field(query.getReduceOn()));
                break;
            case Min:
                searchBuilder.size(0);
                searchBuilder.aggregation(AggregationBuilders.min("min").field(query.getReduceOn()));
                break;
            case Mean:
                searchBuilder.size(0);
                searchBuilder.aggregation(AggregationBuilders.filter("filter").filter(QueryBuilders.existsQuery(query.getReduceOn()))
                        .subAggregation(AggregationBuilders.avg("avg").field(query.getReduceOn())));
                break;
        }
    }

    @Override
    public Set<Object> parseReduce(String result, ReduceQuery query) {
        switch (query.getOp()) {
            case Count:
                if (query.getReduceOn() == null)
                    return getValueByPath(result, "hits.total");
                return getValueByPath(result, "aggregations.count.value");
            case Sum:
                return getValueByPath(result, "aggregations.sum.value");
            case Max:
                return getValueByPath(result, "aggregations.max.value");
            case Min:
                return getValueByPath(result, "aggregations.min.value");
            case Mean:
                Set<Object> count = getValueByPath(result, "aggregations.filter.doc_count");
                Set<Object> sum = getValueByPath(result, "aggregations.filter.avg.value");
                if (count.size() > 0 && sum.size() > 0) {
                    MeanGlobalStep.MeanNumber meanNumber = new MeanGlobalStep.MeanNumber((double) sum.iterator().next(), (long) count.iterator().next());
                    return Collections.singleton(meanNumber);
                }
            default:
                return null;
        }
    }

    protected Set<Object> getValueByPath(String result, String path) {
        String[] split = path.split("\\.");
        try {
            JsonNode jsonNode = mapper.readTree(result);
            for (String part : split)
                jsonNode = jsonNode.get(part);
            double value = jsonNode.asDouble();
            if (value % 1 == 0)
                return Collections.singleton(((Double) value).longValue());
            else
                return Collections.singleton(value);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    protected QueryBuilder createQueryBuilder(PredicatesHolder predicatesHolder) {
        if (predicatesHolder.isAborted()) return null;
        return FilterHelper.createFilterBuilder(predicatesHolder);
    }

    protected SearchSourceBuilder createSearchBuilder(SearchQuery<E> query, QueryBuilder queryBuilder) {
        if (queryBuilder == null) return null;
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(queryBuilder)
                .size(query.getLimit() == -1 ? 10000 : query.getLimit());

        if (query.getPropertyKeys() == null) searchSourceBuilder.fetchSource(true);
        else {
            Set<String> fields = toFields(query.getPropertyKeys());
            if (fields.size() == 0) searchSourceBuilder.fetchSource(false);
            else searchSourceBuilder.fetchSource(fields.toArray(new String[fields.size()]), null);
        }
        List<Pair<String, Order>> orders = query.getOrders();
        if (orders != null) {
            orders.forEach(order -> {
                Order orderValue = order.getValue1();
                switch (orderValue) {
                    case decr:
                        searchSourceBuilder.sort(getFieldByPropertyKey(order.getValue0()), SortOrder.DESC);
                        break;
                    case incr:
                        searchSourceBuilder.sort(getFieldByPropertyKey(order.getValue0()), SortOrder.ASC);
                        break;
                    case shuffle:
                        break;
                }
            });
        }

        return searchSourceBuilder;
    }

    protected Search createSearch(SearchQuery<E> query, QueryBuilder queryBuilder) {
        SearchSourceBuilder searchBuilder = createSearchBuilder(query, queryBuilder);
        if (searchBuilder == null) return null;

        Search.Builder builder = new Search.Builder(searchBuilder.toString().replace("\n", ""))
                .addIndex(index.getIndex(query.getPredicates())).ignoreUnavailable(true).allowNoIndices(true);
        return builder.build();
    }

    @Override
    public List<E> parseResults(String result, PredicateQuery query) {
        List<E> results = new ArrayList<>();
        try {
            JsonNode hits = mapper.readTree(result).get("hits").get("hits");
            for (JsonNode hit : hits) {
                Map<String, Object> source = hit.has("_source") ? mapper.readValue(hit.get("_source").toString(), Map.class) : new HashMap<>();
                Document document = new Document(hit.get("_index").asText(), hit.get("_type").asText(), hit.get("_id").asText(), source);
                Collection<E> elements = fromDocument(document);
                if (elements != null) {
                    elements.forEach(element -> {
                        if (element != null && query.getPredicates().test(element))
                            results.add(element);
                    });
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return results;
    }

    @Override
    public BulkableAction<DocumentResult> addElement(E element, boolean create) {
        Document document = toDocument(element);
        if (document == null) return null;
        Index.Builder builder = new Index.Builder(document.getFields())
                .index(document.getIndex())
                .type(document.getType())
                .id(document.getId());
        if (create) builder.setParameter("op_type", "create");
        return builder.build();
    }

    @Override
    public Delete.Builder delete(E element) {
        Document document = toDocument(element);
        if (document == null) return null;
        return new Delete.Builder(document.getId()).index(document.getIndex()).type(document.getType());
    }

    public Document toDocument(E element) {
        Map<String, Object> fields = this.toFields(element);
        if (fields == null) return null;
        String type = ObjectUtils.firstNonNull(fields.remove("_type"), this.type).toString();
        if (!checkType(type)) return null;
        String id = ObjectUtils.firstNonNull(fields.remove("_id"), fields.remove("id"), element.id()).toString();
        return new Document(index.getIndex(fields), type, id, fields);
    }

    protected Collection<E> fromDocument(Document document) {
        if (!checkType(document.getType())) return null;
        if (!checkIndex(document.getIndex())) return null;
        Map<String, Object> fields = document.getFields();
        fields.put("_id", document.getId());
        fields.put("_type", document.getType());
        return this.fromFields(fields);
    }

    protected boolean checkIndex(String index) {
        return this.index.validateIndex(index);
    }

    protected boolean checkType(String type) {
        return type != null && (this.type == null || this.type.equals(type));
    }

}
