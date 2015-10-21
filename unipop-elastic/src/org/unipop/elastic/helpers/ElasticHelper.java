package org.unipop.elastic.helpers;

import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.elasticsearch.action.admin.cluster.health.*;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.*;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.*;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.*;

import java.io.IOException;
import java.util.*;
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

    public static DeleteByQueryResponse clearIndex(Client client, String indexName){
        DeleteByQueryResponse indexDeleteByQueryResponses = client.prepareDeleteByQuery(indexName).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();

        GetMappingsResponse getMappingsResponse = client.admin().indices().prepareGetMappings(indexName).execute().actionGet();
        ArrayList<String> mappings = new ArrayList();
        getMappingsResponse.getMappings().forEach(map -> {
            map.value.forEach(map2 -> mappings.add(map2.value.type()));
        });

        if(mappings.size() > 0) {
            DeleteMappingResponse deleteMappingResponse = client.admin().indices().prepareDeleteMapping(indexName).setType(mappings.toArray(new String[mappings.size()])).execute().actionGet();
        }

        return indexDeleteByQueryResponses;
    }

    public static BoolFilterBuilder createFilterBuilder(List<HasContainer> hasContainers) {
        BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();
        if(hasContainers != null) hasContainers.forEach(has -> addFilter(boolFilter, has));
        return boolFilter;
    }

    private static void addFilter(BoolFilterBuilder boolFilterBuilder, HasContainer has){
        String key = has.getKey();
        Object value = has.getValue();
        BiPredicate<?, ?> predicate = has.getBiPredicate();

        if(key.equals("~id")) {
            IdsFilterBuilder idsFilterBuilder = FilterBuilders.idsFilter();
            if(value.getClass().isArray()) {
                for(Object id : (Object[])value)
                    idsFilterBuilder.addIds(id.toString());
            }
            else idsFilterBuilder.addIds(value.toString());
            boolFilterBuilder.must(idsFilterBuilder);
        }
        else if(key.equals("~label")) {
            if(value instanceof List){
                List labels = (List) value;
                if(labels.size() == 1)
                    boolFilterBuilder.must(FilterBuilders.typeFilter(labels.get(0).toString()));
                else {
                    FilterBuilder[] filters = new FilterBuilder[labels.size()];
                    for(int i = 0; i < labels.size(); i++)
                        filters[i] = FilterBuilders.typeFilter(labels.get(i).toString());
                    boolFilterBuilder.must(FilterBuilders.orFilter(filters));
                }
            }
            else boolFilterBuilder.must(FilterBuilders.typeFilter(value.toString()));
        }
        else if (predicate instanceof Compare) {
            String predicateString = predicate.toString();
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
                case("inside"):
                    List items =(List) value;
                    Object firstItem = items.get(0);
                    Object secondItem = items.get(1);
                    boolFilterBuilder.must(FilterBuilders.rangeFilter(key).from(firstItem).to(secondItem));
                    break;
                default:
                    throw new IllegalArgumentException("predicate not supported in has step: " + predicate.toString());
            }
        } else if (predicate instanceof Contains) {
            if (predicate == Contains.without) boolFilterBuilder.must(FilterBuilders.missingFilter(key));
            else if (predicate == Contains.within){
                if(value == null) boolFilterBuilder.must(FilterBuilders.existsFilter(key));
                else  boolFilterBuilder.must(FilterBuilders.termsFilter (key, value));
            }
        } else if (predicate instanceof Geo) boolFilterBuilder.must(new GeoShapeFilterBuilder(key, GetShapeBuilder(value), ((Geo) predicate).getRelation()));
        else throw new IllegalArgumentException("predicate not supported by unipop: " + predicate.toString());
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
