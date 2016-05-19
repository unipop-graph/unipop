package org.unipop.common.property.impl;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.unipop.common.property.PropertySchema;
import org.unipop.common.util.ConversionUtils;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.util.*;
import java.util.stream.Collectors;

public class FieldPropertySchema implements PropertySchema {
    private String key;
    private String field = null;
    protected Set include;
    protected Set exclude;

    public FieldPropertySchema(String key, String field) {
        this.key = key;
        this.field = field;
    }

    public FieldPropertySchema(String key, JSONObject config) {
        this.key = key;
        this.field = config.getString("field");
        JSONArray include = config.optJSONArray("include");
        if(include != null) this.include = ConversionUtils.toSet(include);
        JSONArray exclude = config.optJSONArray("exclude");
        if(exclude != null) this.exclude = ConversionUtils.toSet(exclude);
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        Object value = source.get(this.field);
        if(value != null &&
            (include == null || include.contains(value)) &&
            (exclude == null || !exclude.contains(value))) {
            return Collections.singletonMap(this.key, value);
        }
        return null;
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> properties) {
        Object prop = properties.remove(this.key);
        if(prop == null || !test(P.eq(prop))) return null;
        return Collections.singletonMap(this.field, prop);
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        PredicatesHolder fieldPredicate = predicatesHolder.getPredicates().stream().filter(has -> !has.getKey().equals(this.key)).map(has -> {
            if (test(has.getPredicate())) {
                HasContainer hasContainer = new HasContainer(this.field, has.getPredicate());
                return PredicatesHolderFactory.predicate(hasContainer);
            } else return PredicatesHolderFactory.abort();
        }).findFirst().orElse(PredicatesHolderFactory.empty());

        Set<PredicatesHolder> children = predicatesHolder.getChildren().stream().map(this::toPredicates).collect(Collectors.toSet());

        children.add(fieldPredicate);
        return PredicatesHolderFactory.and(children);
    }

    private boolean test(P predicate) {
        if(this.include != null){
            for(Object include : this.include) {
                if(predicate.test(include)) return true;
            }
            return false;
        }
        if(this.exclude != null) {
            for (Object exclude : this.exclude) {
                if (predicate.test(exclude)) return false;
            }
            return true;
        }
        return true;
    }
}
