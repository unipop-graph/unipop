package org.unipop.elastic.document.schema.nested;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.searchbox.action.BulkableAction;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Search;
import io.searchbox.core.Update;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.javatuples.Pair;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.elastic.common.ElasticClient;
import org.unipop.elastic.document.Document;
import org.unipop.elastic.document.DocumentEdgeSchema;
import org.unipop.elastic.document.schema.AbstractDocEdgeSchema;
import org.unipop.elastic.document.schema.AbstractDocSchema;
import org.unipop.elastic.document.schema.DocVertexSchema;
import org.unipop.elastic.document.schema.property.IndexPropertySchema;
import org.unipop.query.UniQuery;
import org.unipop.query.VertexQuery;
import org.unipop.query.aggregation.AggregateVertexQuery;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.query.aggregation.ReduceVertexQuery;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.SearchQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.element.ElementSchema;
import org.unipop.schema.element.VertexSchema;
import org.unipop.schema.property.AbstractPropertyContainer;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniGraph;
import org.unipop.util.ConversionUtils;

import java.util.*;
import java.util.stream.Collectors;

public class NestedEdgeSchema extends AbstractDocEdgeSchema {
    private String path;
    private final DocVertexSchema parentVertexSchema;
    private final VertexSchema childVertexSchema;
    private final Direction parentDirection;

    public NestedEdgeSchema(DocVertexSchema parentVertexSchema, Direction parentDirection, IndexPropertySchema index, String type, String path, JSONObject configuration, ElasticClient client, UniGraph graph) throws JSONException {
        super(configuration, client, graph);
        this.index = index;
        this.type = type;
        this.path = path;
        this.parentVertexSchema = parentVertexSchema;
        this.parentDirection = parentDirection;
        this.childVertexSchema = createVertexSchema("vertex");
        index.addValidation((indexName) -> client.validateNested(indexName, type, path));

    }

    protected VertexSchema createVertexSchema(String key) throws JSONException {
        JSONObject vertexConfiguration = this.json.optJSONObject(key);
        if (vertexConfiguration == null) return null;
        if (vertexConfiguration.optBoolean("ref", false)) return new NestedReferenceVertexSchema(vertexConfiguration, path, graph);
        return new NestedVertexSchema(vertexConfiguration, path, index, type, client, graph);
    }

    @Override
    public Set<ElementSchema> getChildSchemas() {
        return Sets.newHashSet(childVertexSchema);
    }

    @Override
    public Collection<Edge> fromFields(Map<String, Object> fields) {
        Object pathValue = fields.get(this.path);
        if (pathValue == null) return null;

        Vertex parentVertex = parentVertexSchema.createElement(fields);
        if (parentVertex == null) return null;

        if (pathValue instanceof Collection) {
            List<Edge> edges = new ArrayList<>(((Collection) pathValue).size());
            Collection<Map<String, Object>> edgesFields = (Collection<Map<String, Object>>) pathValue;
            for (Map<String, Object> edgeFields : edgesFields) {
                UniEdge edge = createEdge(parentVertex, edgeFields);
                if (edge == null) continue;
                edges.add(edge);
            }
            return edges;
        } else if (pathValue instanceof Map) {
            Map<String, Object> edgeFields = (Map<String, Object>) pathValue;
            UniEdge edge = createEdge(parentVertex, edgeFields);
            return Collections.singleton(edge);
        }
        return null;
    }
    private UniEdge createEdge(Vertex parentVertex, Map<String, Object> edgeFields) {
        Map<String, Object> edgeProperties = getProperties(edgeFields);
        if (edgeProperties == null) return null;
        Vertex childVertex = childVertexSchema.createElement(edgeFields);
        if (childVertex == null) return null;
        UniEdge edge = new UniEdge(edgeProperties,
                parentDirection.equals(Direction.OUT) ? parentVertex : childVertex,
                parentDirection.equals(Direction.IN) ? parentVertex : childVertex, graph);
        return edge;
    }

    @Override
    public Map<String, Object> toFields(Edge edge) {
        Map<String, Object> parentFields = getVertexFields(edge, parentDirection);
        if (parentFields == null) return null;
        Map<String, Object> edgeFields = getFields(edge);
        Map<String, Object> childFields = getVertexFields(edge, parentDirection.opposite());
        Map<String, Object> nestedFields = ConversionUtils.merge(Lists.newArrayList(edgeFields, childFields), this::mergeFields, false);
        if (nestedFields == null) return null;
        parentFields.put(this.path, new Object[]{nestedFields});
        return parentFields;
    }

    private Map<String, Object> getVertexFields(Edge edge, Direction direction) {
        VertexSchema vertexSchema = direction.equals(parentDirection) ? parentVertexSchema : childVertexSchema;
        Vertex parent = edge.vertices(direction).next();
        return vertexSchema.toFields(parent);
    }

    @Override
    public Set<String> toFields(Set<String> propertyKeys) {
        Set<String> fields = super.toFields(propertyKeys).stream()
                .map(key -> path + "." + key).collect(Collectors.toSet());
        Set<String> parentFields = parentVertexSchema.toFields(propertyKeys);
        fields.addAll(parentFields);
        Set<String> childFields = childVertexSchema.toFields(propertyKeys);
        fields.addAll(childFields);
        return fields;
    }

    @Override
    public String getFieldByPropertyKey(String key) {
        String field = super.getFieldByPropertyKey(key);
        if (field == null) return field;
        return path + "." + field;
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        return super.toPredicates(predicatesHolder).map(has -> new HasContainer(path + "." + has.getKey(), has.getPredicate()));
    }

    @Override
    public Search getSearch(SearchQuery<Edge> query) {
        PredicatesHolder predicatesHolder = this.toPredicates(query.getPredicates());
        QueryBuilder queryBuilder = createQueryBuilder(predicatesHolder);
        if(queryBuilder == null)  return null;
        NestedQueryBuilder nestedQuery = QueryBuilders.nestedQuery(this.path, queryBuilder);
        return createSearch(query, nestedQuery);
    }


    @Override
    protected AggregationBuilder getSubAggregation(UniQuery query, AbstractAggregationBuilder builder, Direction direction) {
        VertexQuery searchQuery = (VertexQuery) query;
        PredicatesHolder edgePredicates = super.toPredicates(((PredicateQuery) searchQuery).getPredicates());
        PredicatesHolder VertexPredicates = this.getVertexPredicates(searchQuery.getVertices(), direction);
        QueryBuilder vertexQuery = createQueryBuilder(PredicatesHolderFactory.and(edgePredicates, VertexPredicates));
        if (builder == null)
            return AggregationBuilders.filter("filter").filter(vertexQuery)
                    .subAggregation(AggregationBuilders.reverseNested("reverse"));
        return AggregationBuilders.filter("filter").filter(vertexQuery)
                .subAggregation(AggregationBuilders.reverseNested("reverse")
                        .subAggregation(builder));
    }

//    @Override
//    public Search getLocal(LocalQuery query) {
//        VertexSchema outVertexSchema = parentDirection.equals(Direction.OUT) ? parentVertexSchema : childVertexSchema;
//        VertexSchema inVertexSchema = parentDirection.equals(Direction.IN) ? parentVertexSchema : childVertexSchema;
//        SearchVertexQuery searchQuery = (SearchVertexQuery) query.getSearchQuery();
//        PredicatesHolder edgePredicates = this.toPredicates(searchQuery.getPredicates());
//        PredicatesHolder vertexPredicates = this.getVertexPredicates(searchQuery.getVertices(), searchQuery.getDirection());
//        PredicatesHolder predicatesHolder = PredicatesHolderFactory.and(edgePredicates, vertexPredicates);
//        if (predicatesHolder.isAborted()) return null;
//        QueryBuilder queryBuilder = createQueryBuilder(predicatesHolder);
//        Iterator<String> fields;
//        List<AggregationBuilder> aggs = new ArrayList<>();
//        if (searchQuery.getDirection().equals(Direction.OUT) || searchQuery.getDirection().equals(Direction.BOTH)) {
//            fields = ((AbstractPropertyContainer) outVertexSchema).getPropertySchemas().stream()
//                    .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
//                    .findFirst().get().toFields(Collections.emptySet()).iterator();
//            AggregationBuilder out = createTerms("out", fields);
//            PredicatesHolder outVertexPredicates = this.getVertexPredicates(searchQuery.getVertices(), Direction.OUT);
//            QueryBuilder outQuery = createQueryBuilder(PredicatesHolderFactory.and(edgePredicates, outVertexPredicates));
//            out.subAggregation(AggregationBuilders.filter("filter").filter(outQuery).subAggregation(AggregationBuilders.topHits("hits").setSize(searchQuery.getLimit())));
//            aggs.add(out);
//        }
//        if(searchQuery.getDirection().equals(Direction.IN) || searchQuery.getDirection().equals(Direction.BOTH)){
//            fields = ((AbstractPropertyContainer) inVertexSchema).getPropertySchemas().stream()
//                    .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
//                    .findFirst().get().toFields(Collections.emptySet()).iterator();
//            AggregationBuilder in = createTerms("in", fields);
//            PredicatesHolder inVertexPredicates = this.getVertexPredicates(searchQuery.getVertices(), Direction.IN);
//            QueryBuilder outQuery = createQueryBuilder(PredicatesHolderFactory.and(edgePredicates, inVertexPredicates));
//            in.subAggregation(AggregationBuilders.filter("filter").filter(outQuery).subAggregation(AggregationBuilders.topHits("hits").setSize(searchQuery.getLimit())));
//            aggs.add(in);
//        }
//        SearchSourceBuilder searchBuilder = this.createSearchBuilder(searchQuery, queryBuilder);
//        aggs.forEach(terms -> searchBuilder.aggregation(terms));
//        searchBuilder.size(0);
//        Search.Builder search = new Search.Builder(searchBuilder.toString().replace("\n", ""))
//                .addIndex(index.getIndex(searchQuery.getPredicates()))
//                .ignoreUnavailable(true)
//                .allowNoIndices(true);
//
//        if (type != null)
//            search.addType(type);
//
//        return search.build();
//    }

    @Override
    protected AggregationBuilder createTerms(String name, AggregationBuilder subs, VertexQuery searchQuery, Direction direction, Iterator<String> fields){
        String next = fields.next();
        if (next.equals("_id")) next = "_uid";
        else next = path + "." + next;
        AggregationBuilder agg = AggregationBuilders.nested(name).path(path);
        AggregationBuilder sub = AggregationBuilders.terms(name + "_id").field(next);
        agg.subAggregation(sub);
        if (fields.hasNext()) {
            while (fields.hasNext()) {
                next = fields.next();
                if (next.equals("_id")) next = "_uid";
                else next = path + "." + next;
                TermsBuilder field = AggregationBuilders.terms(name + "_id").field(next);
                sub.subAggregation(field);
                sub = field;
            }
        }
        if (sub == null) {
            agg.subAggregation(subs);
        }
        else
            sub.subAggregation(subs);

        return agg;
    }

    @Override
    public Collection<Pair<String, Element>> parseLocal(String result, LocalQuery query) {
        SearchVertexQuery searchQuery = (SearchVertexQuery) query.getSearchQuery();
        Set<String> fields;
        ArrayList<Pair<String, Element>> finalResult = new ArrayList<>();
        if (searchQuery.getDirection().equals(Direction.OUT) || searchQuery.getDirection().equals(Direction.BOTH)) {
            fields = ((AbstractPropertyContainer) getOutVertexSchema()).getPropertySchemas().stream()
                    .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                    .findFirst().get().toFields(Collections.emptySet());
            List<Pair<String, Element>> out = parseTerms("aggregations.out", "filter.reverse.hits.hits.hits", "out", query, result, fields);
            out.stream().distinct().forEach(finalResult::add);
        }
        if (searchQuery.getDirection().equals(Direction.IN) || searchQuery.getDirection().equals(Direction.BOTH)) {
            fields = ((AbstractPropertyContainer) getInVertexSchema()).getPropertySchemas().stream()
                    .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                    .findFirst().get().toFields(Collections.emptySet());
            List<Pair<String, Element>> in = parseTerms("aggregations.in", "filter.reverse.hits.hits.hits", "in", query, result, fields);
            in.stream().distinct().forEach(finalResult::add);
        }
        return finalResult;
    }

    @Override
    public VertexSchema getOutVertexSchema() {
        return parentDirection.equals(Direction.OUT) ? parentVertexSchema : childVertexSchema;
    }

    @Override
    public VertexSchema getInVertexSchema() {
        return parentDirection.equals(Direction.IN) ? parentVertexSchema : childVertexSchema;
    }

    @Override
    public Search getSearch(SearchVertexQuery query) {
        QueryBuilder queryBuilder = createQueryBuilder(query);
        return createSearch(query, queryBuilder);
    }

    @Override
    public Search getReduce(ReduceVertexQuery query) {
        return null;
    }

    @Override
    public QueryBuilder createQueryBuilder(SearchVertexQuery query) {
        PredicatesHolder edgePredicates = this.toPredicates(query.getPredicates());
        if(edgePredicates.isAborted()) return  null;

        PredicatesHolder childPredicates = childVertexSchema.toPredicates(query.getVertices());
        childPredicates = PredicatesHolderFactory.and(edgePredicates, childPredicates);
        QueryBuilder childQuery = createNestedQueryBuilder(childPredicates);
        if(query.getDirection().equals(parentDirection.opposite())) {
            if (childPredicates.isAborted()) return null;
            return childQuery;
        } else if (!query.getDirection().equals(Direction.BOTH)) childQuery = null;

        PredicatesHolder parentPredicates = parentVertexSchema.toPredicates(query.getVertices());
        QueryBuilder parentQuery = createQueryBuilder(parentPredicates);
        if(parentQuery != null) {
//            if (parentPredicates.isAborted()) return null;
            QueryBuilder edgeQuery = createNestedQueryBuilder(edgePredicates);
            if (edgeQuery != null) {
                parentQuery = QueryBuilders.andQuery(parentQuery, edgeQuery);
            }
        }
        if(query.getDirection().equals(parentDirection) && parentPredicates.notAborted()) return parentQuery;
        else if(childQuery == null && parentPredicates.notAborted()) return parentQuery;
        else if(parentQuery == null && childPredicates.notAborted()) return childQuery;
        else if(parentPredicates.isAborted() && childPredicates.isAborted()) return null;
        else return QueryBuilders.orQuery(parentQuery, childQuery);
    }

    private QueryBuilder createNestedQueryBuilder(PredicatesHolder nestedPredicates) {
        QueryBuilder nestedQuery = createQueryBuilder(nestedPredicates);
        if(nestedQuery == null)  return null;
        return QueryBuilders.nestedQuery(this.path, nestedQuery);
    }

    @Override
    public BulkableAction<DocumentResult> addElement(Edge edge, boolean create) {
        //TODO: use the 'create' parameter to differentiate between add and update
        Vertex parentVertex = parentDirection.equals(Direction.OUT) ? edge.outVertex() : edge.inVertex();
        Document parentDoc = parentVertexSchema.toDocument(parentVertex);
        if (parentDoc == null) return null;
        Map<String, Object> edgeFields = getFields(edge);
        Map<String, Object> childFields = getVertexFields(edge, parentDirection.opposite());
        Map<String, Object> nestedFields = ConversionUtils.merge(Lists.newArrayList(edgeFields, childFields), this::mergeFields, false);
        if (nestedFields == null) return null;
        Set<String> idField = propertySchemas.stream()
                .map(schema -> schema.toFields(Collections.singleton(T.id.getAccessor()))).findFirst().get();
        try {
            HashMap<String, Object> params = new HashMap<>();
            params.put("nestedDoc", nestedFields);
            params.put("path", path);
            params.put("edgeId", edge.id());
            params.put("idField", idField.iterator().next());
            HashMap<String, Object> docMap = new HashMap<>();
            docMap.put("params", params);
            docMap.put("script", UPDATE_SCRIPT);
            String json = mapper.writeValueAsString(docMap);
            return new Update.Builder(json).index(parentDoc.getIndex()).type(parentDoc.getType()).id(parentDoc.getId()).build();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    static String UPDATE_SCRIPT = "if (!ctx._source.containsKey(path)) {ctx._source[path] = [nestedDoc]}; " +
            "else { items_to_remove = []; ctx._source[path].each { item -> if (item[idField] == edgeId) { items_to_remove.add(item); } };" +
            "items_to_remove.each { item -> ctx._source[path].remove(item) }; ctx._source[path] += nestedDoc;}";

}
