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
        if(value == null || !test(P.eq(value))) return null;
        return Collections.singletonMap(this.key, value);
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> properties) {
        Object value = properties.get(this.key);
        if(value == null || !test(P.eq(value))) return null;
        return Collections.singletonMap(this.field, value);
    }

    @Override
    public PredicatesHolder toPredicates(HasContainer has) {
        if(has.getKey().equals(this.key)) {
            if (test(has.getPredicate())) {
                HasContainer hasContainer = new HasContainer(this.field, has.getPredicate());
                return PredicatesHolderFactory.predicate(hasContainer);
            }
            else return PredicatesHolderFactory.abort();
        }

        return null;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public Set<String> getFields() {
        return Collections.singleton(this.field);
    }

    @Override
    public Set<String> getProperties() {
        return Collections.singleton(this.key);
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
