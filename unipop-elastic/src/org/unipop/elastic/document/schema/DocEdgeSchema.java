package org.unipop.elastic.document.schema;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
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
import org.unipop.query.UniQuery;
import org.unipop.query.VertexQuery;
import org.unipop.query.aggregation.AggregateVertexQuery;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.query.aggregation.ReduceQuery;
import org.unipop.query.aggregation.ReduceVertexQuery;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.SearchQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.element.ElementSchema;
import org.unipop.schema.element.VertexSchema;
import org.unipop.schema.property.AbstractPropertyContainer;
import org.unipop.schema.reference.ReferenceVertexSchema;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniGraph;
import org.unipop.util.ConversionUtils;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DocEdgeSchema extends AbstractDocEdgeSchema {
    protected VertexSchema outVertexSchema;
    protected VertexSchema inVertexSchema;

    public DocEdgeSchema(JSONObject configuration, ElasticClient client, UniGraph graph) throws JSONException {
        super(configuration, client, graph);
        this.outVertexSchema = createVertexSchema("outVertex");
        this.inVertexSchema = createVertexSchema("inVertex");
    }

    protected VertexSchema createVertexSchema(String key) throws JSONException {
        JSONObject vertexConfiguration = this.json.optJSONObject(key);
        if (vertexConfiguration == null) return null;
        if (vertexConfiguration.optBoolean("ref", false)) return new ReferenceVertexSchema(vertexConfiguration, graph);
        return new DocVertexSchema(vertexConfiguration, client, graph);
    }

    @Override
    public Set<ElementSchema> getChildSchemas() {
        return Sets.newHashSet(outVertexSchema, inVertexSchema);
    }

    @Override
    public Collection<Edge> fromFields(Map<String, Object> fields) {
        Map<String, Object> edgeProperties = getProperties(fields);
        if (edgeProperties == null) return null;
        Vertex outVertex = outVertexSchema.createElement(fields);
        if (outVertex == null) return null;
        Vertex inVertex = inVertexSchema.createElement(fields);
        if (inVertex == null) return null;
        UniEdge uniEdge = new UniEdge(edgeProperties, outVertex, inVertex, graph);
        return Collections.singleton(uniEdge);
    }

    @Override
    public Map<String, Object> toFields(Edge edge) {
        Map<String, Object> edgeFields = getFields(edge);
        Map<String, Object> inFields = inVertexSchema.toFields(edge.inVertex());
        Map<String, Object> outFields = outVertexSchema.toFields(edge.outVertex());
        return ConversionUtils.merge(Lists.newArrayList(edgeFields, inFields, outFields), this::mergeFields, false);
    }

    @Override
    public Set<String> toFields(Set<String> propertyKeys) {
        Set<String> fields = super.toFields(propertyKeys);
        Set<String> outFields = outVertexSchema.toFields(propertyKeys);
        fields.addAll(outFields);
        Set<String> inFields = inVertexSchema.toFields(propertyKeys);
        fields.addAll(inFields);
        return fields;
    }

    @Override
    public QueryBuilder getSearch(SearchVertexQuery query) {
        PredicatesHolder edgePredicates = this.toPredicates(query.getPredicates());
        PredicatesHolder vertexPredicates = this.getVertexPredicates(query.getVertices(), query.getDirection());
        PredicatesHolder predicatesHolder = PredicatesHolderFactory.and(edgePredicates, vertexPredicates);
        if (predicatesHolder.isAborted()) return null;
        QueryBuilder queryBuilder = createQueryBuilder(predicatesHolder);
        return queryBuilder;
//        return createSearch(query, queryBuilder);
    }

    @Override
    protected PredicatesHolder getReducePredicates(ReduceQuery query) {
        if (query instanceof ReduceVertexQuery){
            PredicatesHolder reducePredicates = super.getReducePredicates(query);
            PredicatesHolder vertexPredicates = getVertexPredicates(query.getVertices(), ((ReduceVertexQuery) query).getDirection());
            return PredicatesHolderFactory.and(reducePredicates, vertexPredicates);
        }
        return super.getReducePredicates(query);
    }

    @Override
    protected void createReduce(ReduceQuery query, SearchSourceBuilder searchBuilder) {
        if (query instanceof ReduceVertexQuery) {
            ReduceVertexQuery reduceVertexQuery = (ReduceVertexQuery) query;
            searchBuilder.size(0);
            switch (query.getOp()) {
                case Count:
                    AbstractAggregationBuilder builder = null;
                    if (query.getReduceOn() != null) {
                        builder = AggregationBuilders.count("count").field(query.getReduceOn());
                    }
                    if (reduceVertexQuery.getDirection().equals(Direction.OUT) || reduceVertexQuery.getDirection().equals(Direction.BOTH)) {
                        Iterator<String> fields = ((AbstractPropertyContainer) getOutVertexSchema()).getPropertySchemas().stream()
                                .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                                .findFirst().get().toFields(Collections.emptySet()).iterator();
                        searchBuilder.aggregation(createTerms("out", getSubAggregation(query, builder, Direction.OUT), (ReduceVertexQuery) query, Direction.OUT, fields));
                    }
                    if (reduceVertexQuery.getDirection().equals(Direction.IN) || reduceVertexQuery.getDirection().equals(Direction.BOTH)) {
                        Iterator<String> fields = ((AbstractPropertyContainer) getInVertexSchema()).getPropertySchemas().stream()
                                .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                                .findFirst().get().toFields(Collections.emptySet()).iterator();
                        searchBuilder.aggregation(createTerms("in", getSubAggregation(query, builder, Direction.IN), (ReduceVertexQuery) query, Direction.OUT, fields));
                    }
                    break;
                case Sum:
                    if (reduceVertexQuery.getDirection().equals(Direction.OUT) || reduceVertexQuery.getDirection().equals(Direction.BOTH)) {
                        Iterator<String> fields = ((AbstractPropertyContainer) getOutVertexSchema()).getPropertySchemas().stream()
                                .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                                .findFirst().get().toFields(Collections.emptySet()).iterator();
                        searchBuilder.aggregation(createTerms("out", getSubAggregation(query, AggregationBuilders.sum("sum").field(query.getReduceOn()), Direction.OUT), (ReduceVertexQuery) query, Direction.OUT, fields));
                    }
                    if (reduceVertexQuery.getDirection().equals(Direction.IN) || reduceVertexQuery.getDirection().equals(Direction.BOTH)) {
                        Iterator<String> fields = ((AbstractPropertyContainer) getInVertexSchema()).getPropertySchemas().stream()
                                .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                                .findFirst().get().toFields(Collections.emptySet()).iterator();
                        searchBuilder.aggregation(createTerms("in", getSubAggregation(query, AggregationBuilders.sum("sum").field(query.getReduceOn()), Direction.IN), (ReduceVertexQuery) query, Direction.OUT, fields));
                    }
                    break;
                case Max:
                    if (reduceVertexQuery.getDirection().equals(Direction.OUT) || reduceVertexQuery.getDirection().equals(Direction.BOTH)) {
                        Iterator<String> fields = ((AbstractPropertyContainer) getOutVertexSchema()).getPropertySchemas().stream()
                                .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                                .findFirst().get().toFields(Collections.emptySet()).iterator();
                        searchBuilder.aggregation(createTerms("out", getSubAggregation(query, AggregationBuilders.max("max").field(query.getReduceOn()), Direction.OUT), (ReduceVertexQuery) query, Direction.OUT, fields));
                    }
                    if (reduceVertexQuery.getDirection().equals(Direction.IN) || reduceVertexQuery.getDirection().equals(Direction.BOTH)) {
                        Iterator<String> fields = ((AbstractPropertyContainer) getInVertexSchema()).getPropertySchemas().stream()
                                .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                                .findFirst().get().toFields(Collections.emptySet()).iterator();
                        searchBuilder.aggregation(createTerms("in", getSubAggregation(query, AggregationBuilders.max("max").field(query.getReduceOn()), Direction.IN), (ReduceVertexQuery) query, Direction.OUT, fields));
                    }
                    break;
                case Min:
                    if (reduceVertexQuery.getDirection().equals(Direction.OUT) || reduceVertexQuery.getDirection().equals(Direction.BOTH)) {
                        Iterator<String> fields = ((AbstractPropertyContainer) getOutVertexSchema()).getPropertySchemas().stream()
                                .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                                .findFirst().get().toFields(Collections.emptySet()).iterator();
                        searchBuilder.aggregation(createTerms("out", getSubAggregation(query, AggregationBuilders.min("min").field(query.getReduceOn()), Direction.OUT), (ReduceVertexQuery) query, Direction.OUT, fields));
                    }
                    if (reduceVertexQuery.getDirection().equals(Direction.IN) || reduceVertexQuery.getDirection().equals(Direction.BOTH)) {
                        Iterator<String> fields = ((AbstractPropertyContainer) getInVertexSchema()).getPropertySchemas().stream()
                                .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                                .findFirst().get().toFields(Collections.emptySet()).iterator();
                        searchBuilder.aggregation(createTerms("in", getSubAggregation(query, AggregationBuilders.min("min").field(query.getReduceOn()), Direction.IN), (ReduceVertexQuery) query, Direction.OUT, fields));
                    }
                    break;
                case Mean:
                    if (reduceVertexQuery.getDirection().equals(Direction.OUT) || reduceVertexQuery.getDirection().equals(Direction.BOTH)) {
                        Iterator<String> fields = ((AbstractPropertyContainer) getOutVertexSchema()).getPropertySchemas().stream()
                                .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                                .findFirst().get().toFields(Collections.emptySet()).iterator();
                        searchBuilder.aggregation(createTerms("out", getSubAggregation(query, AggregationBuilders.filter("filter").filter(QueryBuilders.existsQuery(query.getReduceOn()))
                                .subAggregation(AggregationBuilders.avg("avg").field(query.getReduceOn())), Direction.OUT), (ReduceVertexQuery) query, Direction.OUT, fields));
                    }
                    if (reduceVertexQuery.getDirection().equals(Direction.IN) || reduceVertexQuery.getDirection().equals(Direction.BOTH)) {
                        Iterator<String> fields = ((AbstractPropertyContainer) getInVertexSchema()).getPropertySchemas().stream()
                                .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                                .findFirst().get().toFields(Collections.emptySet()).iterator();
                        searchBuilder.aggregation(createTerms("in", getSubAggregation(query, AggregationBuilders.filter("filter").filter(QueryBuilders.existsQuery(query.getReduceOn()))
                                .subAggregation(AggregationBuilders.avg("avg").field(query.getReduceOn())), Direction.IN), (ReduceVertexQuery) query, Direction.OUT, fields));
                    }
                    break;
            }
        }
        else
            super.createReduce(query, searchBuilder);
    }

    @Override
    public List<Object> parseReduce(String result, ReduceQuery query) {
        if (query instanceof ReduceVertexQuery) {
            List<Object> reduceResult = new ArrayList<>();
            ReduceVertexQuery reduceVertexQuery = (ReduceVertexQuery) query;
            Map<String, Long> idBulk = reduceVertexQuery.getVertices().stream().map(e -> e.id().toString()).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
            switch (query.getOp()) {
                case Count:
                    if (reduceVertexQuery.getDirection().equals(Direction.OUT) || reduceVertexQuery.getDirection().equals(Direction.BOTH)) {
                        List<String> fields = ((AbstractPropertyContainer) getOutVertexSchema()).getPropertySchemas().stream()
                                .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                                .findFirst().get().toFields(Collections.emptySet()).stream().collect(Collectors.toList());
                        if (query.getReduceOn() != null)
                            parseReduce("aggregations", "filter.count.value", "out", result, fields, idBulk, Direction.OUT).forEach(reduceResult::add);
                        else
                            parseReduce("aggregations", "filter.doc_count", "out", result, fields, idBulk, Direction.OUT).forEach(reduceResult::add);
                    }
                    if (reduceVertexQuery.getDirection().equals(Direction.IN) || reduceVertexQuery.getDirection().equals(Direction.BOTH)) {
                        List<String> fields = ((AbstractPropertyContainer) getInVertexSchema()).getPropertySchemas().stream()
                                .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                                .findFirst().get().toFields(Collections.emptySet()).stream().collect(Collectors.toList());
                        if (query.getReduceOn() != null)
                            parseReduce("aggregations", "filter.count.value", "in", result, fields, idBulk, Direction.IN).forEach(reduceResult::add);
                        else
                            parseReduce("aggregations", "filter.doc_count", "in", result, fields, idBulk, Direction.IN).forEach(reduceResult::add);
                    }
                    break;
                case Sum:
                    if (reduceVertexQuery.getDirection().equals(Direction.OUT) || reduceVertexQuery.getDirection().equals(Direction.BOTH)) {
                        List<String> fields = ((AbstractPropertyContainer) getOutVertexSchema()).getPropertySchemas().stream()
                                .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                                .findFirst().get().toFields(Collections.emptySet()).stream().collect(Collectors.toList());
                        parseReduce("aggregations", "filter.sum.value", "out", result, fields, idBulk, Direction.OUT).forEach(reduceResult::add);
                    }
                    if (reduceVertexQuery.getDirection().equals(Direction.IN) || reduceVertexQuery.getDirection().equals(Direction.BOTH)) {
                        List<String> fields = ((AbstractPropertyContainer) getInVertexSchema()).getPropertySchemas().stream()
                                .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                                .findFirst().get().toFields(Collections.emptySet()).stream().collect(Collectors.toList());
                        parseReduce("aggregations", "filter.sum.value", "in", result, fields, idBulk, Direction.IN).forEach(reduceResult::add);
                    }
                    break;
                case Max:
                    if (reduceVertexQuery.getDirection().equals(Direction.OUT) || reduceVertexQuery.getDirection().equals(Direction.BOTH)) {
                        List<String> fields = ((AbstractPropertyContainer) getOutVertexSchema()).getPropertySchemas().stream()
                                .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                                .findFirst().get().toFields(Collections.emptySet()).stream().collect(Collectors.toList());
                        parseReduce("aggregations", "filter.max.value", "out", result, fields, null, Direction.OUT).forEach(reduceResult::add);
                    }
                    if (reduceVertexQuery.getDirection().equals(Direction.IN) || reduceVertexQuery.getDirection().equals(Direction.BOTH)) {
                        List<String> fields = ((AbstractPropertyContainer) getInVertexSchema()).getPropertySchemas().stream()
                                .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                                .findFirst().get().toFields(Collections.emptySet()).stream().collect(Collectors.toList());
                        parseReduce("aggregations", "filter.max.value", "in", result, fields, null, Direction.IN).forEach(reduceResult::add);
                    }
                    break;
                case Min:
                    if (reduceVertexQuery.getDirection().equals(Direction.OUT) || reduceVertexQuery.getDirection().equals(Direction.BOTH)) {
                        List<String> fields = ((AbstractPropertyContainer) getOutVertexSchema()).getPropertySchemas().stream()
                                .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                                .findFirst().get().toFields(Collections.emptySet()).stream().collect(Collectors.toList());
                        parseReduce("aggregations", "filter.min.value", "out", result, fields, null, Direction.OUT).forEach(reduceResult::add);
                    }
                    if (reduceVertexQuery.getDirection().equals(Direction.IN) || reduceVertexQuery.getDirection().equals(Direction.BOTH)) {
                        List<String> fields = ((AbstractPropertyContainer) getInVertexSchema()).getPropertySchemas().stream()
                                .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                                .findFirst().get().toFields(Collections.emptySet()).stream().collect(Collectors.toList());
                        parseReduce("aggregations", "filter.min.value", "in", result, fields, null, Direction.IN).forEach(reduceResult::add);
                    }
                    break;
                case Mean:
                    if (reduceVertexQuery.getDirection().equals(Direction.OUT) || reduceVertexQuery.getDirection().equals(Direction.BOTH)) {
                        List<String> fields = ((AbstractPropertyContainer) getOutVertexSchema()).getPropertySchemas().stream()
                                .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                                .findFirst().get().toFields(Collections.emptySet()).stream().collect(Collectors.toList());
                        ;
                        parseReduce("aggregations", "filter.filter.avg.value", "out", result, fields, idBulk, Direction.OUT).forEach(reduceResult::add);
                    }
                    if (reduceVertexQuery.getDirection().equals(Direction.IN) || reduceVertexQuery.getDirection().equals(Direction.BOTH)) {
                        List<String> fields = ((AbstractPropertyContainer) getInVertexSchema()).getPropertySchemas().stream()
                                .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                                .findFirst().get().toFields(Collections.emptySet()).stream().collect(Collectors.toList());
                        ;
                        parseReduce("aggregations", "filter.filter.avg.value", "in", result, fields, idBulk, Direction.IN).forEach(reduceResult::add);
                    }
                    break;
            }
            return reduceResult;
        }
        else{
            return super.parseReduce(result, query);
        }
    }

    protected List<Pair<HashMap<String, Object>, JsonNode>> getAllBuckets(JsonNode node, String key, List<String> fields, int start) {
        if(node.get("buckets").size() == 0) return Collections.emptyList();
        if (!node.get("buckets").get(0).has(key)) {
            ArrayNode buckets = (ArrayNode) node.get("buckets");
            return ConversionUtils.asStream(buckets.iterator()).map(j -> {
                HashMap<String, Object> map = new HashMap<>();
                map.put(fields.get(start), j.get("key").asText());
                return Pair.with(map, j);
            }).collect(Collectors.toList());
        }
        List<Pair<HashMap<String, Object>, JsonNode>> results = new ArrayList<>();
        ArrayNode buckets = (ArrayNode) node.get("buckets");
        for (JsonNode bucket : buckets) {
            String bucketKey = bucket.get("key").asText();
            List<Pair<HashMap<String, Object>, JsonNode>> allBuckets = getAllBuckets(bucket.get(key), key, fields, start + 1);
            allBuckets.stream().map(p -> {
                p.getValue0().put(fields.get(start), bucketKey);
                return p;
            }).forEach(results::add);
        }
        return results;
    }

    protected List<Object> parseReduce(String path, String bottomPath, String name, String result, List<String> fields, Map<String, Long> idBulk, Direction direction) {
        try {
            JsonNode jsonNode = mapper.readTree(result);
            String[] split = path.split("\\.");
            for (String s : split) {
                jsonNode = jsonNode.get(s);
            }
            List<Pair<HashMap<String, Object>, JsonNode>> allBuckets = getAllBuckets(jsonNode.get(name + "_id"), name + "_id", fields, 0);
            ArrayList<Object> objects = new ArrayList<>();
            for (Pair<HashMap<String, Object>, JsonNode> pair : allBuckets) {
                String id;
                if (direction.equals(Direction.OUT))
                    id = getOutVertexSchema().getPropertySchema(T.id.getAccessor()).toProperties(pair.getValue0()).get(T.id.getAccessor()).toString();
                else
                    id = getInVertexSchema().getPropertySchema(T.id.getAccessor()).toProperties(pair.getValue0()).get(T.id.getAccessor()).toString();
                JsonNode node = pair.getValue1();
                JsonNode parse = null;
                String[] bottomSplit = bottomPath.split("\\.");
                for (String s : bottomSplit) {
                    if (parse == null)
                        parse = node.get(s);
                    else
                        parse = parse.get(s);
                }
                if (parse.isDouble())
                    if (idBulk != null)
                        objects.add(parse.asDouble() * idBulk.getOrDefault(id, 1L)); // TODO: get real number type and multiply by bulk
                    else
                        objects.add(parse.asDouble());
                else if (idBulk != null)
                    objects.add(parse.asLong() * idBulk.getOrDefault(id, 1L));
                else
                    objects.add(parse.asLong());
            }
            return objects;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    @Override
    protected AggregationBuilder getSubAggregation(UniQuery query, AbstractAggregationBuilder builder, Direction direction) {
        VertexQuery searchQuery = (VertexQuery) query;
        PredicatesHolder edgePredicates = this.toPredicates(((PredicateQuery) searchQuery).getPredicates());
        PredicatesHolder vertexPredicates = this.getVertexPredicates(searchQuery.getVertices(), direction);
        QueryBuilder vertexQuery = createQueryBuilder(PredicatesHolderFactory.and(edgePredicates, vertexPredicates));
        if (builder == null)
            return AggregationBuilders.filter("filter").filter(vertexQuery);
        return AggregationBuilders.filter("filter").filter(vertexQuery).subAggregation(builder);
    }

    @Override
    protected AggregationBuilder createTerms(String name, AggregationBuilder subs, VertexQuery searchQuery, Direction direction, Iterator<String> fields) {
        String next = fields.next();
        if (next.equals("_id")) next = "_uid";
        AggregationBuilder agg = AggregationBuilders.terms(name + "_id").field(next);
        AggregationBuilder sub = null;
        if (fields.hasNext()) {
            sub = AggregationBuilders.terms(name + "_id").field(next);
            agg.subAggregation(sub);
            while (fields.hasNext()) {
                next = fields.next();
                if (next.equals("_id")) next = "_uid";
                TermsBuilder field = AggregationBuilders.terms(name + "_id_").field(next);
                sub.subAggregation(field);
                sub = field;
            }
        }
        if (sub == null) {
            agg.subAggregation(subs);
        } else
            sub.subAggregation(subs);

        return agg;
    }

    @Override
    public Collection<Pair<String, Element>> parseLocal(String result, LocalQuery query) {
        SearchVertexQuery searchQuery = (SearchVertexQuery) query.getSearchQuery();
        Set<String> fields;
        ArrayList<Pair<String, Element>> finalResult = new ArrayList<>();
        if (searchQuery.getDirection().equals(Direction.OUT) || searchQuery.getDirection().equals(Direction.BOTH)) {
            fields = ((AbstractPropertyContainer) outVertexSchema).getPropertySchemas().stream()
                    .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                    .findFirst().get().toFields(Collections.emptySet());
            List<Pair<String, Element>> out = parseTerms("aggregations", "filter.hits.hits.hits", "out", query, result, fields);
            out.forEach(finalResult::add);
        }
        if (searchQuery.getDirection().equals(Direction.IN) || searchQuery.getDirection().equals(Direction.BOTH)) {
            fields = ((AbstractPropertyContainer) inVertexSchema).getPropertySchemas().stream()
                    .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                    .findFirst().get().toFields(Collections.emptySet());
            List<Pair<String, Element>> in = parseTerms("aggregations", "filter.hits.hits.hits", "in", query, result, fields);
            in.forEach(finalResult::add);
        }
        return finalResult;
    }

    @Override
    public VertexSchema getOutVertexSchema() {
        return outVertexSchema;
    }

    @Override
    public VertexSchema getInVertexSchema() {
        return inVertexSchema;
    }

    @Override
    public String toString() {
        return "DocEdgeSchema{" +
                "index='" + null + '\'' +
                ", type='" + type + '\'' +
                '}';
    }

}
