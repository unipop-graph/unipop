package org.unipop.elastic.helpers;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.*;
import org.javatuples.Pair;
import org.unipop.controller.ExistsP;
import org.unipop.process.traversal.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiPredicate;

public class ElasticHelper {

    public static void createIndex(String indexName, Client client) throws IOException {
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
            throw new IOException(clusterHealth.getStatus() +
                    " status returned from cluster '" + client.admin().cluster().toString() +
                    "', index '" + indexName + "'");

        }
    }

    public static DeleteByQueryResponse clearIndex(Client client, String indexName) {
        DeleteByQueryResponse indexDeleteByQueryResponses = client.prepareDeleteByQuery(indexName).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();

        GetMappingsResponse getMappingsResponse = client.admin().indices().prepareGetMappings(indexName).execute().actionGet();
        ArrayList<String> mappings = new ArrayList();
        getMappingsResponse.getMappings().forEach(map -> {
            map.value.forEach(map2 -> mappings.add(map2.value.type()));
        });

        if (mappings.size() > 0) {
            DeleteMappingResponse deleteMappingResponse = client.admin().indices().prepareDeleteMapping(indexName).setType(mappings.toArray(new String[mappings.size()])).execute().actionGet();
        }

        return indexDeleteByQueryResponses;
    }

    public static void mapNested(Client client, String index, String typeName, String nestedFieldName) {
        try {
            XContentBuilder nestedMapping = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject(typeName)
                    .startObject("properties")
                    .startObject(nestedFieldName)
                    .field("type", "nested")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            client.admin().indices().preparePutMapping(index).setType(typeName).setSource(nestedMapping).execute().actionGet();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public static QueryBuilder createQuery(List<HasContainer> hasContainers, FilterBuilder... filtersToAdd) {
        BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();
        List<Pair<QueryBuilder, Boolean>> queryBuilders = new ArrayList<>();
        if (hasContainers != null) hasContainers.forEach(has -> {
            if (has.getBiPredicate() instanceof Text)
                addQuery(queryBuilders, has);
            addFilter(boolFilter, has);
        });

        for (FilterBuilder filterBuilder : filtersToAdd) {
            boolFilter.must(filterBuilder);
        }

        if (queryBuilders.isEmpty()) {
            if (boolFilter.hasClauses())
                return QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolFilter);
            else
                return QueryBuilders.matchAllQuery();
        } else {
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            queryBuilders.forEach(pair -> {
                if (pair.getValue1())
                    boolQuery.must(pair.getValue0());
                else boolQuery.mustNot(pair.getValue0());
            });
            return QueryBuilders.filteredQuery(boolQuery, boolFilter);
        }
    }

    private static void addQuery(List<Pair<QueryBuilder, Boolean>> queryBuilders, HasContainer has) {
        String predicateString = has.getBiPredicate().toString();
        switch (predicateString) {
            case "LIKE":
                queryBuilders.add(new Pair<>(QueryBuilders.wildcardQuery(has.getKey(), has.getValue().toString()), true));
                break;
            case "UNLIKE":
                queryBuilders.add(new Pair<>(QueryBuilders.wildcardQuery(has.getKey(), has.getValue().toString()), false));
                break;
            case "REGEXP":
                queryBuilders.add(new Pair<>(QueryBuilders.regexpQuery(has.getKey(), has.getValue().toString()), true));
                break;
            case "UNREGEXP":
                queryBuilders.add(new Pair<>(QueryBuilders.regexpQuery(has.getKey(), has.getValue().toString()), false));
                break;
            case "FUZZY":
                queryBuilders.add(new Pair<>(QueryBuilders.fuzzyQuery(has.getKey(), has.getValue().toString()), true));
                break;
            case "UNFUZZY":
                queryBuilders.add(new Pair<>(QueryBuilders.fuzzyQuery(has.getKey(), has.getValue().toString()), false));
                break;
            default:
                throw new IllegalArgumentException("predicate not supported in has step: " + has.getBiPredicate().toString());
        }
    }

    public static BoolFilterBuilder createFilterBuilder(List<HasContainer> hasContainers) {
        BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();
        if(hasContainers != null) hasContainers.forEach(has -> addFilter(boolFilter, has));
        return boolFilter;
    }

    private static void addFilter(BoolFilterBuilder boolFilterBuilder, HasContainer has) {
        String key = has.getKey();
        Object value = has.getValue();
        BiPredicate<?, ?> biPredicate = has.getBiPredicate();

        if (key.equals(T.id.getAccessor())) {
            IdsFilterBuilder idsFilterBuilder = FilterBuilders.idsFilter();
            if (value instanceof Iterable) {
                for (Object id : (Iterable) value)
                    idsFilterBuilder.addIds(id.toString());
            } else idsFilterBuilder.addIds(value.toString());
            boolFilterBuilder.must(idsFilterBuilder);
        } else if (key.equals(T.label.getAccessor())) {
            if (value instanceof List) {
                List labels = (List) value;
                if (labels.size() == 1)
                    boolFilterBuilder.must(FilterBuilders.typeFilter(labels.get(0).toString()));
                else {
                    FilterBuilder[] filters = new FilterBuilder[labels.size()];
                    for (int i = 0; i < labels.size(); i++)
                        filters[i] = FilterBuilders.typeFilter(labels.get(i).toString());
                    boolFilterBuilder.must(FilterBuilders.orFilter(filters));
                }
            } else boolFilterBuilder.must(FilterBuilders.typeFilter(value.toString()));
        } else if (biPredicate != null) {
            if (biPredicate instanceof Compare) {
                String predicateString = biPredicate.toString();
                switch (predicateString) {
                    case ("eq"):
                        boolFilterBuilder.must(FilterBuilders.termFilter(key, value));
                        break;
                    case ("neq"):
                        boolFilterBuilder.mustNot(FilterBuilders.termFilter(key, value));
                        break;
                    case ("gt"):
                        boolFilterBuilder.must(FilterBuilders.rangeFilter(key).gt(value));
                        break;
                    case ("gte"):
                        boolFilterBuilder.must(FilterBuilders.rangeFilter(key).gte(value));
                        break;
                    case ("lt"):
                        boolFilterBuilder.must(FilterBuilders.rangeFilter(key).lt(value));
                        break;
                    case ("lte"):
                        boolFilterBuilder.must(FilterBuilders.rangeFilter(key).lte(value));
                        break;
                    case ("inside"):
                        List items = (List) value;
                        Object firstItem = items.get(0);
                        Object secondItem = items.get(1);
                        boolFilterBuilder.must(FilterBuilders.rangeFilter(key).from(firstItem).to(secondItem));
                        break;
                    default:
                        throw new IllegalArgumentException("predicate not supported in has step: " + biPredicate.toString());
                }
            } else if (biPredicate instanceof Contains) {
                if (biPredicate == Contains.without) boolFilterBuilder.must(FilterBuilders.missingFilter(key));
                else if (biPredicate == Contains.within) {
                    if (value == null) boolFilterBuilder.must(FilterBuilders.existsFilter(key));
                    else if (value instanceof Iterable)
                        boolFilterBuilder.must(FilterBuilders.termsFilter(key, (Iterable) value));
                    else boolFilterBuilder.must(FilterBuilders.termsFilter(key, value));
                }
            } else if (biPredicate instanceof Geo)
                boolFilterBuilder.must(new GeoShapeFilterBuilder(key, GetShapeBuilder(value), ((Geo) biPredicate).getRelation()));
            else if (biPredicate instanceof Text) {

            } else throw new IllegalArgumentException("predicate not supported by unipop: " + biPredicate.toString());
        } else if (has.getPredicate() instanceof ExistsP) {
            boolFilterBuilder.must(FilterBuilders.existsFilter(key));
        } else {
            //todo: add descriptive unsupported has container description
            throw new IllegalArgumentException("HasContainer not supported by unipop");
        }
    }

    private static ShapeBuilder GetShapeBuilder(Object object) {
        try {
            String geoJson = (String) object;
            XContentParser parser = JsonXContent.jsonXContent.createParser(geoJson);
            parser.nextToken();

            return ShapeBuilder.parse(parser);
        } catch (Exception e) {
            return null;
        }
    }
}
