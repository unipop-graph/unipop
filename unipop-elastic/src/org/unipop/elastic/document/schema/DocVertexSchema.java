package org.unipop.elastic.document.schema;

import io.searchbox.core.Search;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.node.ArrayNode;
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
import org.unipop.elastic.document.DocumentVertexSchema;
import org.unipop.elastic.document.schema.nested.NestedEdgeSchema;
import org.unipop.query.UniQuery;
import org.unipop.query.VertexQuery;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.query.aggregation.ReduceQuery;
import org.unipop.query.aggregation.ReduceVertexQuery;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.schema.element.EdgeSchema;
import org.unipop.schema.element.ElementSchema;
import org.unipop.schema.property.AbstractPropertyContainer;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;
import org.unipop.util.ConversionUtils;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.unipop.util.ConversionUtils.getList;

public class DocVertexSchema extends AbstractDocSchema<Vertex> implements DocumentVertexSchema {
    protected Set<ElementSchema> edgeSchemas = new HashSet<>();

    public DocVertexSchema(JSONObject configuration, ElasticClient client, UniGraph graph) throws JSONException {
        super(configuration, client, graph);

        for (JSONObject edgeJson : getList(json, "edges")) {
            EdgeSchema docEdgeSchema = getEdgeSchema(edgeJson);
            edgeSchemas.add(docEdgeSchema);
        }
    }

    private EdgeSchema getEdgeSchema(JSONObject edgeJson) throws JSONException {
        String path = edgeJson.optString("path", null);
        Direction direction = Direction.valueOf(edgeJson.optString("direction"));

        if (path == null) return new InnerEdgeSchema(this, direction, index, type, edgeJson, client, graph);
        return new NestedEdgeSchema(this, direction, index, type, path, edgeJson, client, graph);
    }

    @Override
    public Search getSearch(DeferredVertexQuery query) {
        PredicatesHolder predicatesHolder = this.toPredicates(query.getVertices());
        QueryBuilder queryBuilder = createQueryBuilder(predicatesHolder);
        return createSearch(query, queryBuilder);
    }

    protected AggregationBuilder getSubAggregation(UniQuery query, AbstractAggregationBuilder builder) {
        PredicatesHolder vertexPredicates = this.toPredicates(((PredicateQuery) query).getPredicates());
        QueryBuilder vertexQuery = createQueryBuilder(vertexPredicates);
        if (builder == null)
            return AggregationBuilders.filter("filter").filter(vertexQuery);
        return AggregationBuilders.filter("filter").filter(vertexQuery).subAggregation(builder);
    }

    protected AggregationBuilder createTerms(String name, AggregationBuilder subs, Iterator<String> fields) {
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
    protected void createReduce(ReduceQuery query, SearchSourceBuilder searchBuilder) {
        searchBuilder.size(0);
        Iterator<String> fields;
        switch (query.getOp()) {
            case Count:
                AbstractAggregationBuilder builder = null;
                if (query.getReduceOn() != null) {
                    builder = AggregationBuilders.count("count").field(query.getReduceOn());
                }
                fields = getPropertySchemas().stream()
                        .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                        .findFirst().get().toFields(Collections.emptySet()).iterator();
                searchBuilder.aggregation(createTerms("vertex", getSubAggregation(query, builder), fields));
                break;
            case Sum:
                fields = getPropertySchemas().stream()
                        .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                        .findFirst().get().toFields(Collections.emptySet()).iterator();
                searchBuilder.aggregation(createTerms("vertex", getSubAggregation(query, AggregationBuilders.sum("sum").field(query.getReduceOn())), fields));
                break;
            case Max:
                fields = getPropertySchemas().stream()
                        .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                        .findFirst().get().toFields(Collections.emptySet()).iterator();
                searchBuilder.aggregation(createTerms("vertex", getSubAggregation(query, AggregationBuilders.max("max").field(query.getReduceOn())), fields));
                break;
            case Min:
                fields = getPropertySchemas().stream()
                        .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                        .findFirst().get().toFields(Collections.emptySet()).iterator();
                searchBuilder.aggregation(createTerms("vertex", getSubAggregation(query, AggregationBuilders.min("min").field(query.getReduceOn())), fields));
                break;
            case Mean:
                fields = getPropertySchemas().stream()
                        .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                        .findFirst().get().toFields(Collections.emptySet()).iterator();
                searchBuilder.aggregation(createTerms("vertex", getSubAggregation(query, AggregationBuilders.filter("filter").filter(QueryBuilders.existsQuery(query.getReduceOn()))
                        .subAggregation(AggregationBuilders.avg("avg").field(query.getReduceOn()))), fields));
                break;
        }
    }

    @Override
    public List<Object> parseReduce(String result, ReduceQuery query) {
        List<Object> reduceResult = new ArrayList<>();
        Map<String, Long> idBulk = query.getVertices().stream().map(e -> e.id().toString()).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        List<String> fields;
        switch (query.getOp()) {
            case Count:
                fields = getPropertySchemas().stream()
                        .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                        .findFirst().get().toFields(Collections.emptySet()).stream().collect(Collectors.toList());
                if (query.getReduceOn() != null)
                    parseReduce("aggregations", "filter.count.value", "vertex", result, fields, idBulk).forEach(reduceResult::add);
                else
                    parseReduce("aggregations", "filter.doc_count", "vertex", result, fields, idBulk).forEach(reduceResult::add);

                break;
            case Sum:
                fields = getPropertySchemas().stream()
                        .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                        .findFirst().get().toFields(Collections.emptySet()).stream().collect(Collectors.toList());
                parseReduce("aggregations", "filter.sum.value", "vertex", result, fields, idBulk).forEach(reduceResult::add);
                break;
            case Max:
                fields = getPropertySchemas().stream()
                        .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                        .findFirst().get().toFields(Collections.emptySet()).stream().collect(Collectors.toList());
                parseReduce("aggregations", "filter.max.value", "vertex", result, fields, null).forEach(reduceResult::add);
                break;
            case Min:
                fields = getPropertySchemas().stream()
                        .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                        .findFirst().get().toFields(Collections.emptySet()).stream().collect(Collectors.toList());
                parseReduce("aggregations", "filter.min.value", "vertex", result, fields, null).forEach(reduceResult::add);
                break;
            case Mean:
                fields = getPropertySchemas().stream()
                        .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                        .findFirst().get().toFields(Collections.emptySet()).stream().collect(Collectors.toList());;
                parseReduce("aggregations", "filter.filter.avg.value", "vertex", result, fields, idBulk).forEach(reduceResult::add);
                break;
        }
        return reduceResult;
    }

    protected List<Pair<HashMap<String, Object>, JsonNode>> getAllBuckets(JsonNode node, String key, List<String> fields, int start) {
        if (!node.get("buckets").get(0).has(key)) {
            ArrayNode buckets = (ArrayNode) node.get("buckets");
            return ConversionUtils.asStream(buckets.iterator()).map(j -> {
                HashMap<String, Object> map = new HashMap<>();
                if (fields.get(start).equals("_id"))
                    map.put(fields.get(start), j.get("key").asText().split("#")[1]);
                else map.put(fields.get(start), j.get("key").asText());
                return Pair.with(map, j);
            }).collect(Collectors.toList());
        }
        List<Pair<HashMap<String, Object>, JsonNode>> results = new ArrayList<>();
        ArrayNode buckets = (ArrayNode) node.get("buckets");
        for (JsonNode bucket : buckets) {
            String bucketKey = fields.get(start).equals("_id") ?
                    bucket.get("key").asText().split("#")[1] :
                    bucket.get("key").asText();
            List<Pair<HashMap<String, Object>, JsonNode>> allBuckets = getAllBuckets(bucket.get(key), key, fields, start + 1);
            allBuckets.stream().map(p -> {
                p.getValue0().put(fields.get(start), bucketKey);
                return p;
            }).forEach(results::add);
        }
        return results;
    }

    protected List<Object> parseReduce(String path, String bottomPath, String name, String result, List<String> fields, Map<String, Long> idBulk) {
        try {
            JsonNode jsonNode = mapper.readTree(result);
            String[] split = path.split("\\.");
            for (String s : split) {
                jsonNode = jsonNode.get(s);
            }
            List<Pair<HashMap<String, Object>, JsonNode>> allBuckets = getAllBuckets(jsonNode.get(name + "_id"), name + "_id", fields, 0);
            ArrayList<Object> objects = new ArrayList<>();
            for (Pair<HashMap<String, Object>, JsonNode> pair : allBuckets) {
                String id = getPropertySchema(T.id.getAccessor()).toProperties(pair.getValue0()).get(T.id.getAccessor()).toString();
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
                        objects.add(parse.asDouble() * idBulk.get(id)); // TODO: get real number type and multiply by bulk
                    else
                        objects.add(parse.asDouble());
                else if (idBulk != null)
                    objects.add(parse.asLong() * idBulk.get(id));
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
    protected PredicatesHolder getReducePredicates(ReduceQuery query) {
        if (query.getVertices().equals(Collections.emptyList()))
            return super.getReducePredicates(query);
        return PredicatesHolderFactory.and(super.getReducePredicates(query), this.toPredicates(query.getVertices()));
    }

    @Override
    public PredicatesHolder getVertexPredicates(List<Vertex> vertices) {
        return this.toPredicates(vertices);
    }

    @Override
    public Vertex createElement(Map<String, Object> fields) {
        Map<String, Object> properties = getProperties(fields);
        if (properties == null) return null;
        return new UniVertex(properties, graph);
    }

    @Override
    public Set<ElementSchema> getChildSchemas() {
        return this.edgeSchemas;
    }

    @Override
    public String toString() {
        return "DocVertexSchema{" +
                "index='" + null + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
