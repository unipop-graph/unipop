package org.unipop.elastic2.helpers;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.*;
import org.unipop.controller.ExistsP;

import java.io.IOException;
import java.util.List;
import java.util.function.BiPredicate;

/**
 * Elastic helper class.
 */
public class ElasticHelper {


    ////////////////////////////////////////////////////////////////////////////
    // Methods

    /**
     * Creates index.
     *
     * @param indexName the index name.
     * @param client the client.
     * @throws IOException when times out.
     */
    public static void createIndex(String indexName, Client client) throws IOException {
        IndicesExistsRequest request = new IndicesExistsRequest(indexName);
        IndicesExistsResponse response = client.admin().indices().exists(request).actionGet();
        if (!response.isExists()) {
            Settings settings = Settings.settingsBuilder().put("index.analysis.analyzer.default.type", "keyword").build();
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

    /**
     * Clears the index.
     *
     * @param client the client.
     * @param indexName the index name to be cleared.
     * @return Deleted by query response.
     */
    public static DeleteIndexResponse clearIndex(Client client, String indexName){
        if (client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists()) {
            // 1. Delete index is faster. Also, ES 2.x requires plugin to support delete by query.
            DeleteIndexResponse response = client.admin().indices()
                    .delete(new DeleteIndexRequest(indexName))
                    .actionGet();

            // 2. recreate the index after deletion.
            try {
                createIndex(indexName, client);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return response;
        }
        return null;
    }

    /**
     * Creates a bool filter builder.
     * @param hasContainers the list of has container
     * @return bool filter builder
     */
    public static BoolQueryBuilder createQueryBuilder(List<HasContainer> hasContainers) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        if(hasContainers != null) hasContainers.forEach(has -> addQuery(boolQuery, has));
        return boolQuery;
    }

    /**
     * Adds filter to the filter builder.
     *
     * @param boolQueryBuilder the bool filter builder.
     * @param has the has container.
     */
    private static void addQuery(BoolQueryBuilder boolQueryBuilder, HasContainer has){
        String key = has.getKey();
        Object value = has.getValue();
        BiPredicate<?, ?> biPredicate = has.getBiPredicate();

        if (key.equals(T.id.getAccessor())) {
            IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery();
            if (value instanceof Iterable) {
                for (Object id : (Iterable) value)
                    idsQueryBuilder.addIds(id.toString());
            } else idsQueryBuilder.addIds(value.toString());
            boolQueryBuilder.filter(idsQueryBuilder); 
        } else if (key.equals(T.label.getAccessor())) {
            if (value instanceof List) {
                List labels = (List) value;
                if (labels.size() == 1)
                    boolQueryBuilder.filter(QueryBuilders.typeQuery(labels.get(0).toString())); 
                else {
                    QueryBuilder[] filters = new QueryBuilder[labels.size()];
                    for (int i = 0; i < labels.size(); i++)
                        filters[i] = QueryBuilders.typeQuery(labels.get(i).toString());
                    boolQueryBuilder.filter(QueryBuilders.orQuery(filters)); 
                }
            } else boolQueryBuilder.filter(QueryBuilders.typeQuery(value.toString())); 
        } else if (biPredicate != null) {
            if (biPredicate instanceof Compare) {
                String predicateString = biPredicate.toString();
                switch (predicateString) {
                    case ("eq"):
                        boolQueryBuilder.filter(QueryBuilders.termQuery(key, value));
                        break;
                    case ("neq"):
                        boolQueryBuilder.mustNot(QueryBuilders.termQuery(key, value));
                        break;
                    case ("gt"):
                        boolQueryBuilder.filter(QueryBuilders.rangeQuery(key).gt(value));
                        break;
                    case ("gte"):
                        boolQueryBuilder.filter(QueryBuilders.rangeQuery(key).gte(value));
                        break;
                    case ("lt"):
                        boolQueryBuilder.filter(QueryBuilders.rangeQuery(key).lt(value));
                        break;
                    case ("lte"):
                        boolQueryBuilder.filter(QueryBuilders.rangeQuery(key).lte(value));
                        break;
                    case ("inside"):
                        List items = (List) value;
                        Object firstItem = items.get(0);
                        Object secondItem = items.get(1);
                        boolQueryBuilder.filter(QueryBuilders.rangeQuery(key).from(firstItem).to(secondItem)); 
                        break;
                    default:
                        throw new IllegalArgumentException("predicate not supported in has step: " + biPredicate.toString());
                }
            } else if (biPredicate instanceof Contains) {
                if (biPredicate == Contains.without) boolQueryBuilder.filter(QueryBuilders.missingQuery(key)); 
                else if (biPredicate == Contains.within) {
                    if (value == null) boolQueryBuilder.filter(QueryBuilders.existsQuery(key)); 
                    else if(value instanceof Iterable) boolQueryBuilder.filter(QueryBuilders.termsQuery (key, (Iterable)value)); 
                    else boolQueryBuilder.filter(QueryBuilders.termsQuery(key, value)); 
                }
            } else if (biPredicate instanceof Geo)
                boolQueryBuilder.filter(new GeoShapeQueryBuilder(key, GetShapeBuilder(value), ((Geo) biPredicate).getRelation())); 
            else throw new IllegalArgumentException("predicate not supported by unipop: " + biPredicate.toString());
        }
        else if (has.getPredicate() instanceof ExistsP) {
            boolQueryBuilder.filter(QueryBuilders.existsQuery(key)); 
        } else {
            //todo: add descriptive unsupported has container description
            throw new IllegalArgumentException("HasContainer not supported by unipop");
        }
    }

    /**
     * Gets shape builder.
     *
     * @param object the geo object in JSON format.
     * @return shape builder.
     */
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

    public static void mapNested(Client client, String index, String typeName, String nestedFieldName){
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
}