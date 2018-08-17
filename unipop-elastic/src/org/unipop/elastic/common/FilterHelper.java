package org.unipop.elastic.common;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.*;
import org.unipop.process.predicate.ExistsP;
import org.unipop.process.predicate.Text;
import org.unipop.query.predicates.PredicatesHolder;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

/**
 * TODO: Cleanup and create implementation of PredicatesTranslator with logs.
 */
public class FilterHelper {
    public static QueryBuilder createFilterBuilder(PredicatesHolder predicatesHolder) {

        Set<QueryBuilder> predicateFilters = predicatesHolder.getPredicates().stream()
                .map(FilterHelper::createFilter).collect(Collectors.toSet());
        Set<QueryBuilder> childFilters = predicatesHolder.getChildren().stream()
                .map(FilterHelper::createFilterBuilder).collect(Collectors.toSet());
        predicateFilters.addAll(childFilters);


        if (predicateFilters.size() == 0) return QueryBuilders.matchAllQuery();
        if (predicateFilters.size() == 1) return predicateFilters.iterator().next();


        BoolQueryBuilder predicatesQuery = QueryBuilders.boolQuery();
        if (predicatesHolder.getClause().equals(PredicatesHolder.Clause.And)) {
            predicateFilters.forEach(predicatesQuery::must);
        } else if (predicatesHolder.getClause().equals(PredicatesHolder.Clause.Or)) {
            predicateFilters.forEach(predicatesQuery::should);
        } else throw new IllegalArgumentException("Unexpected clause in predicatesHolder: " + predicatesHolder);

        return QueryBuilders.constantScoreQuery(predicatesQuery);
    }

    public static QueryBuilder createFilter(HasContainer container) {
        String key = container.getKey();
        P predicate = container.getPredicate();
        Object value = predicate.getValue();
        BiPredicate<?, ?> biPredicate = predicate.getBiPredicate();
        if (key.equals("_id")) return getIdsFilter(value);
        else if (key.equals("_type")) return getTypeFilter(container);
        else if (predicate instanceof ConnectiveP) {
            return handleConnectiveP(key, (ConnectiveP) predicate);
        } else if (biPredicate != null) {
            return predicateToQuery(key, value, biPredicate);
        } else if (predicate instanceof ExistsP) return QueryBuilders.existsQuery(key);
        else throw new IllegalArgumentException("HasContainer not supported by unipop");
    }

    private static QueryBuilder handleConnectiveP(String key, ConnectiveP predicate){
        List<P> predicates = predicate.getPredicates();
        List<QueryBuilder> queries = predicates.stream().map(p -> {
            if (p instanceof ConnectiveP) return handleConnectiveP(key, (ConnectiveP) p);
            Object pValue = p.getValue();
            BiPredicate pBiPredicate = p.getBiPredicate();
            return predicateToQuery(key, pValue, pBiPredicate);
        }).collect(Collectors.toList());

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if (predicate instanceof AndP)
            queries.forEach(boolQueryBuilder::must);
        else if (predicate instanceof OrP)
            queries.forEach(boolQueryBuilder::should);
        else throw new IllegalArgumentException("Connective predicate not supported by unipop");

        return boolQueryBuilder;
    }

    private static QueryBuilder predicateToQuery(String key, Object value, BiPredicate<?, ?> biPredicate) {
        String keyWordkey = key;
        if (biPredicate instanceof Compare) return getCompareFilter(keyWordkey, value, biPredicate.toString());
        else if (biPredicate instanceof Contains) return getContainsFilter(keyWordkey, value, biPredicate);
//        else if (biPredicate instanceof Geo) return getGeoFilter(key, value, (Geo) biPredicate);
        else if (biPredicate instanceof Text.TextPredicate)
            return getTextFilter(keyWordkey, value, (Text.TextPredicate) biPredicate);
        else throw new IllegalArgumentException("predicate not supported by unipop: " + biPredicate.toString());
    }

    private static QueryBuilder getTextFilter(String key, Object value, Text.TextPredicate biPredicate) {
        String predicate = biPredicate.toString();
        switch (predicate) {
            case "LIKE":
                return QueryBuilders.wildcardQuery(key, value.toString());
            case "UNLIKE":
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.wildcardQuery(key, value.toString()));
            case "PREFIX":
                return QueryBuilders.prefixQuery(key, value.toString());
            case "UNPREFIX":
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.prefixQuery(key, value.toString()));
            case "REGEXP":
                return QueryBuilders.regexpQuery(key, value.toString());
            case "UNREGEXP":
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.regexpQuery(key, value.toString()));
            case "FUZZY":
                return QueryBuilders.fuzzyQuery(key, value.toString());
            case "UNFUZZY":
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.fuzzyQuery(key, value.toString()));
            default:
                throw new IllegalArgumentException("predicate not supported in has step: " + predicate);
        }
    }

    private static QueryBuilder getTypeFilter(HasContainer has) {
        BiPredicate<?, ?> biPredicate = has.getBiPredicate();
        if (biPredicate instanceof Compare) {
            QueryBuilder query = QueryBuilders.typeQuery(has.getValue().toString());
            if (biPredicate.equals(Compare.eq)) return query;
            return QueryBuilders.boolQuery().mustNot(query);
        } else if (biPredicate instanceof Contains) {
            Collection values = (Collection) has.getValue();
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            boolean within = biPredicate.equals(Contains.within);
            values.forEach(label -> {
                TypeQueryBuilder typeQueryBuilder = QueryBuilders.typeQuery(label.toString());
                if (within) boolQueryBuilder.should(typeQueryBuilder);
                else boolQueryBuilder.mustNot(typeQueryBuilder);
            });
            return boolQueryBuilder;
        } else throw new IllegalArgumentException("predicate not supported by unipop: " + biPredicate.toString());
    }

    private static QueryBuilder getIdsFilter(Object value) {
        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery();
        if (value instanceof Iterable) {
            for (Object id : (Iterable) value)
                idsQueryBuilder.addIds(id.toString());
        } else idsQueryBuilder.addIds(value.toString());
        return idsQueryBuilder;
    }

    private static QueryBuilder getCompareFilter(String key, Object value, String predicateString) {
        switch (predicateString) {
            case ("eq"):
                return QueryBuilders.termQuery(key, value);
            case ("neq"):
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(key, value));
            case ("gt"):
                return QueryBuilders.rangeQuery(key).gt(value);
            case ("gte"):
                return QueryBuilders.rangeQuery(key).gte(value);
            case ("lt"):
                return QueryBuilders.rangeQuery(key).lt(value);
            case ("lte"):
                return QueryBuilders.rangeQuery(key).lte(value);
            case ("inside"):
                List items = (List) value;
                Object firstItem = items.get(0);
                Object secondItem = items.get(1);
                return QueryBuilders.rangeQuery(key).from(firstItem).to(secondItem);
            default:
                throw new IllegalArgumentException("predicate not supported in has step: " + predicateString);
        }
    }

    private static QueryBuilder getContainsFilter(String key, Object value, BiPredicate<?, ?> biPredicate) {
        if (biPredicate == Contains.without) return QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(key));
        else if (biPredicate == Contains.within) {
            if (value == null) return QueryBuilders.existsQuery(key);
            else if (value instanceof Collection<?>)
                return QueryBuilders.termsQuery(key, (Collection<?>) value);
            else if (value.getClass().isArray())
                return QueryBuilders.termsQuery(key, (Object[]) value);
            else return QueryBuilders.termsQuery(key, value);
        } else throw new IllegalArgumentException("predicate not supported by unipop: " + biPredicate.toString());
    }

//    private static QueryBuilder getGeoFilter(String key, Object value, Geo biPredicate) {
//        try {
//            String geoJson = value.toString();
//            XContentParser parser = JsonXContent.jsonXContent.createParser(geoJson);
//            parser.nextToken();
//
//            ShapeBuilder shape = ShapeBuilder.parse(parser);
//            return new GeoShapeQueryBuilder(key, shape, biPredicate.getRelation());
//        } catch (Exception e) {
//            return null;
//        }
//    }
}