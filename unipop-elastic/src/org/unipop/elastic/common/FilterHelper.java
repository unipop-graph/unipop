package org.unipop.elastic.common;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.*;
import org.unipop.process.predicate.ExistsP;
import org.unipop.query.predicates.PredicatesHolder;

import java.util.List;
import java.util.function.BiPredicate;

public class FilterHelper {
    public static FilterBuilder createFilterBuilder(PredicatesHolder predicatesHolder) {
        if(predicatesHolder.getChildren().size() == 0 && predicatesHolder.getPredicates().size() == 1)
            return createFilter(predicatesHolder.getPredicates().stream().findFirst().get());
        if(predicatesHolder.getChildren().size() == 1 && predicatesHolder.getPredicates().size() == 0)
            return createFilterBuilder(predicatesHolder.getChildren().stream().findFirst().get());

        BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();

        predicatesHolder.getPredicates().forEach(has -> {
            FilterBuilder filter = createFilter(has);
            addFilter(boolFilter, filter, predicatesHolder.getClause());
        });
        predicatesHolder.getChildren().forEach(childPredicates -> {
            FilterBuilder filter = createFilterBuilder(childPredicates);
            addFilter(boolFilter, filter, predicatesHolder.getClause());
        });

        return boolFilter;
    }

    private static void addFilter(BoolFilterBuilder boolFilter, FilterBuilder filter, PredicatesHolder.Clause clause) {
        if(clause.equals(PredicatesHolder.Clause.And)) boolFilter.must(filter);
        else boolFilter.should(filter);
    }

    public static FilterBuilder createFilter(HasContainer container) {
        String key = container.getKey();
        P predicate = container.getPredicate();
        Object value = predicate.getValue();
        BiPredicate<?, ?> biPredicate = predicate.getBiPredicate();
        if (key.equals(T.id.getAccessor())) return getIdsFilter(value);
        else if (key.equals(T.label.getAccessor())) return getTypeFilter(value);
        else if (biPredicate != null) {
            if (biPredicate instanceof Compare) return getCompareFilter(key, value, biPredicate.toString());
            else if (biPredicate instanceof Contains) return getContainsFilter(key, value, biPredicate);
            else if (biPredicate instanceof Geo) return getGeoFilter(key, value, (Geo) biPredicate);
            else throw new IllegalArgumentException("predicate not supported by unipop: " + biPredicate.toString());
        }
        else if (predicate instanceof ExistsP) return FilterBuilders.existsFilter(key);
        else throw new IllegalArgumentException("HasContainer not supported by unipop");
    }

    private static FilterBuilder getTypeFilter(Object value) {
        if (value instanceof List) {
            List labels = (List) value;
            if (labels.size() == 1)
                return FilterBuilders.typeFilter(labels.get(0).toString());
            else {
                FilterBuilder[] filters = new FilterBuilder[labels.size()];
                for (int i = 0; i < labels.size(); i++)
                    filters[i] = FilterBuilders.typeFilter(labels.get(i).toString());
                return FilterBuilders.orFilter(filters);
            }
        } else return FilterBuilders.typeFilter(value.toString());
    }

    private static FilterBuilder getIdsFilter(Object value) {
        IdsFilterBuilder idsFilterBuilder = FilterBuilders.idsFilter();
        if (value instanceof Iterable) {
            for (Object id : (Iterable) value)
                idsFilterBuilder.addIds(id.toString());
        } else idsFilterBuilder.addIds(value.toString());
        return idsFilterBuilder;
    }

    private static FilterBuilder getCompareFilter(String key, Object value, String predicateString) {
        switch (predicateString) {
            case ("eq"):
                return FilterBuilders.termFilter(key, value);
            case ("neq"):
                return FilterBuilders.notFilter(FilterBuilders.termFilter(key, value));
            case ("gt"):
                return FilterBuilders.rangeFilter(key).gt(value);
            case ("gte"):
                return FilterBuilders.rangeFilter(key).gte(value);
            case ("lt"):
                return FilterBuilders.rangeFilter(key).lt(value);
            case ("lte"):
                return FilterBuilders.rangeFilter(key).lte(value);
            case ("inside"):
                List items = (List) value;
                Object firstItem = items.get(0);
                Object secondItem = items.get(1);
                return FilterBuilders.rangeFilter(key).from(firstItem).to(secondItem);
            default:
                throw new IllegalArgumentException("predicate not supported in has step: " + predicateString);
        }
    }

    private static FilterBuilder getContainsFilter(String key, Object value, BiPredicate<?, ?> biPredicate) {
        if (biPredicate == Contains.without) return FilterBuilders.missingFilter(key);
        else if (biPredicate == Contains.within) {
            if (value == null) return FilterBuilders.existsFilter(key);
            else if (value instanceof Iterable)
                return FilterBuilders.termsFilter(key, (Iterable) value);
            else return FilterBuilders.termsFilter(key, value);
        }
        else throw new IllegalArgumentException("predicate not supported by unipop: " + biPredicate.toString());
    }

    private static FilterBuilder getGeoFilter(String key, Object value, Geo biPredicate) {
        try {
            String geoJson = value.toString();
            XContentParser parser = JsonXContent.jsonXContent.createParser(geoJson);
            parser.nextToken();

            ShapeBuilder shape = ShapeBuilder.parse(parser);
            return new GeoShapeFilterBuilder(key, shape, biPredicate.getRelation());
        } catch (Exception e) {
            return null;
        }
    }
}
