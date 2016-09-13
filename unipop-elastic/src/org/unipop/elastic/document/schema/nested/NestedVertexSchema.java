package org.unipop.elastic.document.schema.nested;

import io.searchbox.action.BulkableAction;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Search;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.support.QueryInnerHitBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.nested.NestedBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.javatuples.Pair;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.elastic.common.ElasticClient;
import org.unipop.elastic.common.FilterHelper;
import org.unipop.elastic.document.DocumentVertexSchema;
import org.unipop.elastic.document.schema.AbstractDocSchema;
import org.unipop.elastic.document.schema.property.IndexPropertySchema;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.query.aggregation.ReduceQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;
import org.unipop.util.ConversionUtils;

import java.util.*;
import java.util.stream.Collectors;

public class NestedVertexSchema extends AbstractDocSchema<Vertex> implements DocumentVertexSchema {
    private String path;

    public NestedVertexSchema(JSONObject configuration, String path, IndexPropertySchema index, String type, ElasticClient client, UniGraph graph) throws JSONException {
        super(configuration, client, graph);
        this.path = path;
        this.index = index;
        this.type = type;
    }

    @Override
    protected Map<String, Object> getFields(Vertex element) {
        Map<String, Object> properties = UniElement.fullProperties(element);
        List<Map<String, Object>> fieldMaps = this.getPropertySchemas().stream().map(schema ->
                schema.toFields(properties)).filter(prop -> prop != null).collect(Collectors.toList());
        return ConversionUtils.merge(fieldMaps, this::mergeFields, false);
    }

    @Override
    public Set<String> toFields(Set<String> propertyKeys) {
        return super.toFields(propertyKeys).stream().map(key -> path + "." + key).collect(Collectors.toSet());
    }

    @Override
    public String getFieldByPropertyKey(String key) {
        String field = super.getFieldByPropertyKey(key);
        if (field == null) return field;
        return path + "." + field;
    }

    @Override
    public Collection<Vertex> fromFields(Map<String, Object> fields) {
        Object pathValue = fields.get(this.path);
        if (pathValue == null) return null;
        if (pathValue instanceof Collection) {
            List<Vertex> vertices = new ArrayList<>(((Collection) pathValue).size());
            Collection<Map<String, Object>> verticesFields = (Collection<Map<String, Object>>) pathValue;
            verticesFields.forEach(vertexField -> {
                Vertex vertex = createElement(vertexField);
                if (vertex != null)
                    vertices.add(vertex);
            });
            return vertices;
        } else if (pathValue instanceof Map) {
            Map<String, Object> vertexField = (Map<String, Object>) pathValue;
            Vertex vertex = createElement(vertexField);
            if (vertex != null)
                return Collections.singleton(vertex);
        }
        return null;
    }

    @Override
    public Vertex createElement(Map<String, Object> fields) {
        Map<String, Object> properties = getProperties(fields);
        if (properties == null) return null;
        return new UniVertex(properties, graph);
    }

    @Override
    protected void createReduce(ReduceQuery query, SearchSourceBuilder searchBuilder) {
        switch (query.getOp()) {
            case Count:
                searchBuilder.size(0);
                NestedBuilder nested = AggregationBuilders.nested("nested").path(path);
                if (query.getReduceOn() != null)
                    nested.subAggregation(AggregationBuilders.count("count").field(query.getReduceOn()));
                searchBuilder.aggregation(nested);
                break;
            case Sum:
                searchBuilder.size(0);
                searchBuilder.aggregation(AggregationBuilders.nested("nested").path(path)
                        .subAggregation(AggregationBuilders.sum("sum").field(query.getReduceOn())));
                break;
            case Max:
                searchBuilder.size(0);
                searchBuilder.aggregation(AggregationBuilders.nested("nested").path(path)
                        .subAggregation(AggregationBuilders.max("max").field(query.getReduceOn())));
                break;
            case Min:
                searchBuilder.size(0);
                searchBuilder.aggregation(AggregationBuilders.nested("nested").path(path)
                        .subAggregation(AggregationBuilders.min("min").field(query.getReduceOn())));
                break;
            case Mean:
                searchBuilder.size(0);
                searchBuilder.aggregation(AggregationBuilders.nested("nested").path(path)
                        .subAggregation(AggregationBuilders.filter("filter").filter(QueryBuilders.existsQuery(query.getReduceOn()))
                                .subAggregation(AggregationBuilders.avg("avg").field(query.getReduceOn()))));
                break;
        }
    }

    @Override
    public Set<Object> parseReduce(String result, ReduceQuery query) {
        switch (query.getOp()) {
            case Count:
                if (query.getReduceOn() == null)
                    return getValueByPath(result, "aggregations.nested.doc_count");
                return getValueByPath(result, "aggregations.nested.count.value");
            case Sum:
                return getValueByPath(result, "aggregations.nested.sum.value");
            case Max:
                return getValueByPath(result, "aggregations.nested.max.value");
            case Min:
                return getValueByPath(result, "aggregations.nested.min.value");
            case Mean:
                Set<Object> count = getValueByPath(result, "aggregations.nested.filter.doc_count");
                Set<Object> sum = getValueByPath(result, "aggregations.nested.filter.avg.value");
                if (count.size() > 0 && sum.size() > 0) {
                    MeanGlobalStep.MeanNumber meanNumber = new MeanGlobalStep.MeanNumber((double) sum.iterator().next(), (long) count.iterator().next());
                    return Collections.singleton(meanNumber);
                }
            default:
                return null;
        }
    }

    @Override
    public Collection<Pair<String, Element>> parseLocal(String result, LocalQuery query) {
        return null;
    }

    @Override
    public BulkableAction<DocumentResult> addElement(Vertex element, boolean create) {
        return null;
    }

    @Override
    public QueryBuilder createQueryBuilder(PredicatesHolder predicatesHolder) {
        QueryBuilder queryBuilder = super.createQueryBuilder(predicatesHolder);
        if (queryBuilder == null) return null;
        return QueryBuilders.nestedQuery(this.path, queryBuilder);
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        return super.toPredicates(predicatesHolder).map(has -> new HasContainer(path + "." + has.getKey(), has.getPredicate()));
    }

    @Override
    public Search getSearch(DeferredVertexQuery query) {
        PredicatesHolder predicatesHolder = this.toPredicates(query.getVertices());
        QueryBuilder queryBuilder = createQueryBuilder(predicatesHolder);
        return createSearch(query, queryBuilder);
    }

    @Override
    public PredicatesHolder getVertexPredicates(List<Vertex> vertices) {
        return null;
    }
}
