package org.unipop.elastic.common;

import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.ConstantScoreQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.ExistsQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.FuzzyQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.IdsQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchAllQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.PrefixQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.RegexpQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.TermsQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.WildcardQuery;
import co.elastic.clients.json.JsonData;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.unipop.process.predicate.ExistsP;
import org.unipop.process.predicate.Text;
import org.unipop.query.predicates.PredicatesHolder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

/**
 * TODO: Cleanup and create implementation of PredicatesTranslator with logs.
 */
public class FilterHelper {
    public static Query createFilterBuilder(PredicatesHolder predicatesHolder) {

        Set<Query> predicateFilters = predicatesHolder.getPredicates().stream()
                .map(FilterHelper::createFilter).collect(Collectors.toSet());
        Set<Query> childFilters = predicatesHolder.getChildren().stream()
                .map(FilterHelper::createFilterBuilder).collect(Collectors.toSet());
        predicateFilters.addAll(childFilters);


        if (predicateFilters.size() == 0) return MatchAllQuery.of(m -> m)._toQuery();
        if (predicateFilters.size() == 1) return predicateFilters.iterator().next();


        List<Query> must = new ArrayList<>();
        List<Query> should = new ArrayList<>();
        if (predicatesHolder.getClause().equals(PredicatesHolder.Clause.And)) {
            must.addAll(predicateFilters);
        } else if (predicatesHolder.getClause().equals(PredicatesHolder.Clause.Or)) {
            should.addAll(predicateFilters);
        } else throw new IllegalArgumentException("Unexpected clause in predicatesHolder: " + predicatesHolder);

        Query boolQuery = BoolQuery.of(b -> b.must(must).should(should))._toQuery();
        return ConstantScoreQuery.of(c -> c.filter(boolQuery))._toQuery();
    }

    public static Query createFilter(HasContainer container) {
        String key = container.getKey();
        P predicate = container.getPredicate();
        Object value = predicate.getValue();
        BiPredicate<?, ?> biPredicate = predicate.getBiPredicate();
        if (key.equals("_id")) return getIdsFilter(value);
        // _type queries are removed in ES 8 — type discrimination is by index
        else if (key.equals("_type")) return MatchAllQuery.of(m -> m)._toQuery();
        else if (predicate instanceof ConnectiveP) {
            return handleConnectiveP(key, (ConnectiveP) predicate);
        } else if (biPredicate != null) {
            return predicateToQuery(key, value, biPredicate);
        } else if (predicate instanceof ExistsP) return ExistsQuery.of(e -> e.field(key))._toQuery();
        else throw new IllegalArgumentException("HasContainer not supported by unipop");
    }

    private static Query handleConnectiveP(String key, ConnectiveP predicate) {
        List<P> predicates = predicate.getPredicates();
        List<Query> queries = predicates.stream().map(p -> {
            if (p instanceof ConnectiveP) return handleConnectiveP(key, (ConnectiveP) p);
            Object pValue = p.getValue();
            BiPredicate pBiPredicate = p.getBiPredicate();
            return predicateToQuery(key, pValue, pBiPredicate);
        }).collect(Collectors.toList());

        List<Query> must = new ArrayList<>();
        List<Query> should = new ArrayList<>();
        if (predicate instanceof AndP)
            must.addAll(queries);
        else if (predicate instanceof OrP)
            should.addAll(queries);
        else throw new IllegalArgumentException("Connective predicate not supported by unipop");

        return BoolQuery.of(b -> b.must(must).should(should))._toQuery();
    }

    private static Query predicateToQuery(String key, Object value, BiPredicate<?, ?> biPredicate) {
        if (biPredicate instanceof Compare) return getCompareFilter(key, value, biPredicate.toString());
        else if (biPredicate instanceof Contains) return getContainsFilter(key, value, biPredicate);
//        else if (biPredicate instanceof Geo) return getGeoFilter(key, value, (Geo) biPredicate);
        else if (biPredicate instanceof Text.TextPredicate)
            return getTextFilter(key, value, (Text.TextPredicate) biPredicate);
        else throw new IllegalArgumentException("predicate not supported by unipop: " + biPredicate.toString());
    }

    private static Query getTextFilter(String key, Object value, Text.TextPredicate biPredicate) {
        String predicate = biPredicate.toString();
        switch (predicate) {
            case "LIKE":
                return WildcardQuery.of(w -> w.field(key).value(value.toString()))._toQuery();
            case "UNLIKE":
                Query likeQ = WildcardQuery.of(w -> w.field(key).value(value.toString()))._toQuery();
                return BoolQuery.of(b -> b.mustNot(likeQ))._toQuery();
            case "PREFIX":
                return PrefixQuery.of(p -> p.field(key).value(value.toString()))._toQuery();
            case "UNPREFIX":
                Query prefixQ = PrefixQuery.of(p -> p.field(key).value(value.toString()))._toQuery();
                return BoolQuery.of(b -> b.mustNot(prefixQ))._toQuery();
            case "REGEXP":
                return RegexpQuery.of(r -> r.field(key).value(value.toString()))._toQuery();
            case "UNREGEXP":
                Query regexpQ = RegexpQuery.of(r -> r.field(key).value(value.toString()))._toQuery();
                return BoolQuery.of(b -> b.mustNot(regexpQ))._toQuery();
            case "FUZZY":
                return FuzzyQuery.of(f -> f.field(key).value(FieldValue.of(value.toString())))._toQuery();
            case "UNFUZZY":
                Query fuzzyQ = FuzzyQuery.of(f -> f.field(key).value(FieldValue.of(value.toString())))._toQuery();
                return BoolQuery.of(b -> b.mustNot(fuzzyQ))._toQuery();
            default:
                throw new IllegalArgumentException("predicate not supported in has step: " + predicate);
        }
    }

    private static Query getIdsFilter(Object value) {
        List<String> idList = new ArrayList<>();
        if (value instanceof Iterable) {
            for (Object id : (Iterable) value)
                idList.add(id.toString());
        } else idList.add(value.toString());
        return IdsQuery.of(i -> i.values(idList))._toQuery();
    }

    private static Query getCompareFilter(String key, Object value, String predicateString) {
        switch (predicateString) {
            case ("eq"):
                return TermQuery.of(t -> t.field(key).value(FieldValue.of(value)))._toQuery();
            case ("neq"):
                Query termQ = TermQuery.of(t -> t.field(key).value(FieldValue.of(value)))._toQuery();
                return BoolQuery.of(b -> b.mustNot(termQ))._toQuery();
            case ("gt"):
                return RangeQuery.of(r -> r.untyped(u -> u.field(key).gt(JsonData.of(value))))._toQuery();
            case ("gte"):
                return RangeQuery.of(r -> r.untyped(u -> u.field(key).gte(JsonData.of(value))))._toQuery();
            case ("lt"):
                return RangeQuery.of(r -> r.untyped(u -> u.field(key).lt(JsonData.of(value))))._toQuery();
            case ("lte"):
                return RangeQuery.of(r -> r.untyped(u -> u.field(key).lte(JsonData.of(value))))._toQuery();
            case ("inside"):
                List items = (List) value;
                Object firstItem = items.get(0);
                Object secondItem = items.get(1);
                return RangeQuery.of(r -> r.untyped(u -> u.field(key).from(JsonData.of(firstItem)).to(JsonData.of(secondItem))))._toQuery();
            default:
                throw new IllegalArgumentException("predicate not supported in has step: " + predicateString);
        }
    }

    private static Query getContainsFilter(String key, Object value, BiPredicate<?, ?> biPredicate) {
        if (biPredicate == Contains.without)
            return BoolQuery.of(b -> b.mustNot(ExistsQuery.of(e -> e.field(key))._toQuery()))._toQuery();
        else if (biPredicate == Contains.within) {
            if (value == null) return ExistsQuery.of(e -> e.field(key))._toQuery();
            else if (value instanceof Collection<?>) {
                List<FieldValue> fieldValues = ((Collection<?>) value).stream()
                        .map(FieldValue::of)
                        .collect(Collectors.toList());
                return TermsQuery.of(t -> t.field(key).terms(tt -> tt.value(fieldValues)))._toQuery();
            } else if (value.getClass().isArray()) {
                Object[] arr = (Object[]) value;
                List<FieldValue> fieldValues = new ArrayList<>();
                for (Object v : arr) fieldValues.add(FieldValue.of(v));
                return TermsQuery.of(t -> t.field(key).terms(tt -> tt.value(fieldValues)))._toQuery();
            } else {
                List<FieldValue> fieldValues = List.of(FieldValue.of(value));
                return TermsQuery.of(t -> t.field(key).terms(tt -> tt.value(fieldValues)))._toQuery();
            }
        } else throw new IllegalArgumentException("predicate not supported by unipop: " + biPredicate.toString());
    }

//    private static Query getGeoFilter(String key, Object value, Geo biPredicate) {
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
