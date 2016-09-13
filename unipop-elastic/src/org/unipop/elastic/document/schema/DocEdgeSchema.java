package org.unipop.elastic.document.schema;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.searchbox.core.Search;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.node.ArrayNode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.javatuples.Pair;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.elastic.common.ElasticClient;
import org.unipop.elastic.document.Document;
import org.unipop.elastic.document.DocumentEdgeSchema;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.query.aggregation.ReduceVertexQuery;
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

public class DocEdgeSchema extends AbstractDocSchema<Edge> implements DocumentEdgeSchema {
    protected VertexSchema outVertexSchema;
    protected VertexSchema inVertexSchema;

    public DocEdgeSchema(JSONObject configuration, ElasticClient client, UniGraph graph) throws JSONException {
        super(configuration, client, graph);
        this.outVertexSchema = createVertexSchema("outVertex");
        this.inVertexSchema = createVertexSchema("inVertex");
    }

    protected VertexSchema createVertexSchema(String key) throws JSONException {
        JSONObject vertexConfiguration = this.json.optJSONObject(key);
        if(vertexConfiguration == null) return null;
        if(vertexConfiguration.optBoolean("ref", false)) return new ReferenceVertexSchema(vertexConfiguration, graph);
        return new DocVertexSchema(vertexConfiguration, client, graph);
    }

    @Override
    public Set<ElementSchema> getChildSchemas() {
        return Sets.newHashSet(outVertexSchema, inVertexSchema);
    }

    @Override
    public Collection<Edge> fromFields(Map<String, Object> fields) {
        Map<String, Object> edgeProperties = getProperties(fields);
        if(edgeProperties == null) return null;
        Vertex outVertex = outVertexSchema.createElement(fields);
        if(outVertex == null) return null;
        Vertex inVertex = inVertexSchema.createElement(fields);
        if(inVertex == null) return null;
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
    public Search getSearch(SearchVertexQuery query) {
        PredicatesHolder edgePredicates = this.toPredicates(query.getPredicates());
        PredicatesHolder vertexPredicates = this.getVertexPredicates(query.getVertices(), query.getDirection());
        PredicatesHolder predicatesHolder = PredicatesHolderFactory.and(edgePredicates, vertexPredicates);
        if (predicatesHolder.isAborted()) return null;
        QueryBuilder queryBuilder = createQueryBuilder(predicatesHolder);
        return createSearch(query, queryBuilder);
    }

    @Override
    public Search getReduce(ReduceVertexQuery query) {
        PredicatesHolder edgePredicates = this.toPredicates(query.getPredicates());
        PredicatesHolder vertexPredicates = this.getVertexPredicates(query.getVertices(), query.getDirection());
        PredicatesHolder predicatesHolder = PredicatesHolderFactory.and(edgePredicates, vertexPredicates);
        if (predicatesHolder.isAborted()) return null;
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
        SearchVertexQuery searchQuery = (SearchVertexQuery) query.getSearchQuery();
        PredicatesHolder edgePredicates = this.toPredicates(searchQuery.getPredicates());
        PredicatesHolder vertexPredicates = this.getVertexPredicates(searchQuery.getVertices(), searchQuery.getDirection());
        PredicatesHolder predicatesHolder = PredicatesHolderFactory.and(edgePredicates, vertexPredicates);
        if (predicatesHolder.isAborted()) return null;
        QueryBuilder queryBuilder = createQueryBuilder(predicatesHolder);
        Iterator<String> fields;
        List<AggregationBuilder> aggs = new ArrayList<>();
        if (searchQuery.getDirection().equals(Direction.OUT) || searchQuery.getDirection().equals(Direction.BOTH)) {
            fields = ((AbstractPropertyContainer) outVertexSchema).getPropertySchemas().stream()
                    .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                    .findFirst().get().toFields(Collections.emptySet()).iterator();
            AggregationBuilder out = createTerms("out", fields);
            PredicatesHolder outVertexPredicates = this.getVertexPredicates(searchQuery.getVertices(), Direction.OUT);
            QueryBuilder outQuery = createQueryBuilder(PredicatesHolderFactory.and(edgePredicates, outVertexPredicates));
            out.subAggregation(AggregationBuilders.filter("filter").filter(outQuery).subAggregation(AggregationBuilders.topHits("hits").setSize(searchQuery.getLimit())));
            aggs.add(out);
        }
        if(searchQuery.getDirection().equals(Direction.IN) || searchQuery.getDirection().equals(Direction.BOTH)){
            fields = ((AbstractPropertyContainer) inVertexSchema).getPropertySchemas().stream()
                    .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                    .findFirst().get().toFields(Collections.emptySet()).iterator();
            AggregationBuilder in = createTerms("in", fields);
            PredicatesHolder inVertexPredicates = this.getVertexPredicates(searchQuery.getVertices(), Direction.IN);
            QueryBuilder outQuery = createQueryBuilder(PredicatesHolderFactory.and(edgePredicates, inVertexPredicates));
            in.subAggregation(AggregationBuilders.filter("filter").filter(outQuery).subAggregation(AggregationBuilders.topHits("hits").setSize(searchQuery.getLimit())));
            aggs.add(in);
        }
        SearchSourceBuilder searchBuilder = this.createSearchBuilder(searchQuery, queryBuilder);
        aggs.forEach(terms -> searchBuilder.aggregation(terms));
        searchBuilder.size(0);
        Search.Builder search = new Search.Builder(searchBuilder.toString().replace("\n", ""))
                .addIndex(index.getIndex(searchQuery.getPredicates()))
                .ignoreUnavailable(true)
                .allowNoIndices(true);

        if (type != null)
            search.addType(type);

        return search.build();
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
            List<Pair<String, Element>> out = parseTerms("out", query, result, fields);
            out.forEach(finalResult::add);
        }
        if (searchQuery.getDirection().equals(Direction.IN) || searchQuery.getDirection().equals(Direction.BOTH)) {
            fields = ((AbstractPropertyContainer) inVertexSchema).getPropertySchemas().stream()
                    .filter(schema -> schema.getKey().equals(T.id.getAccessor()))
                    .findFirst().get().toFields(Collections.emptySet());
            List<Pair<String, Element>> in = parseTerms("in", query, result, fields);
            in.forEach(finalResult::add);
        }
        return finalResult;
    }

    protected List<Pair<String, Element>> parseTerms(String name, LocalQuery query, String result, Set<String> fields){
        SearchVertexQuery searchQuery = (SearchVertexQuery) query.getSearchQuery();
        try {
            JsonNode jsonNode = mapper.readTree(result);
            for (int i = 1; i < fields.size() + 1; i++) {
                jsonNode = jsonNode.get("aggregations");
                jsonNode = jsonNode.get(name + "_id_" + i);
            }
            ArrayNode buckets = (ArrayNode) jsonNode.get("buckets");
            ArrayList<Pair<String, Element>> objects = new ArrayList<>();
            for (JsonNode node : buckets) {
                ArrayNode hits = (ArrayNode) node.get("filter").get("hits").get("hits").withArray("hits");
                for (JsonNode hit : hits) {
                    Collection<Edge> edges = fromDocument(
                            new Document(hit.get("_index").asText(),
                                    hit.get("_type").asText(),
                                    hit.get("_id").asText(),
                                    mapper.readValue(hit.get("_source").toString(), Map.class)));
                    if (edges != null){
                        edges.forEach(edge -> {
                            if (query.getQueryClass().equals(Edge.class))
                                objects.add(Pair.with(node.get("key").asText(), edge));
                            else{
                                if (searchQuery.getDirection().equals(Direction.OUT))
                                    objects.add(Pair.with(node.get("key").asText(), edge.inVertex()));
                                else if (searchQuery.getDirection().equals(Direction.IN))
                                    objects.add(Pair.with(node.get("key").asText(), edge.outVertex()));
                                else{
                                    objects.add(Pair.with(node.get("key").asText(), edge.inVertex()));
                                    objects.add(Pair.with(node.get("key").asText(), edge.outVertex()));
                                }
                            }
                        });
                    }
                }
            }
            return objects;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    protected PredicatesHolder getVertexPredicates(List<Vertex> vertices, Direction direction) {
        PredicatesHolder outPredicates = this.outVertexSchema.toPredicates(vertices);
        PredicatesHolder inPredicates = this.inVertexSchema.toPredicates(vertices);
        if(direction.equals(Direction.OUT) && outPredicates.notAborted()) return outPredicates;
        if(direction.equals(Direction.IN) && inPredicates.notAborted()) return inPredicates;
        if (outPredicates.notAborted() && inPredicates.notAborted())
            return PredicatesHolderFactory.or(inPredicates, outPredicates);
        else if (outPredicates.isAborted()) return inPredicates;
        else if (inPredicates.isAborted()) return outPredicates;
        else return PredicatesHolderFactory.abort();
    }

    @Override
    public String toString() {
        return "DocEdgeSchema{" +
                "index='" + null + '\'' +
                ", type='" + type + '\'' +
                '}';
    }

}
