package org.unipop.common.property.impl;

import org.json.JSONArray;
import org.json.JSONObject;
import org.unipop.common.property.PropertySchema;
import org.unipop.common.util.ConversionUtils;
import org.unipop.query.predicates.PredicatesHolder;

import java.util.*;

public class DynamicPropertiesSchema implements PropertySchema {

    private Set<String> include;
    private Set<String> exclude;

    public DynamicPropertiesSchema(Set<String> excludeKeys) {
        this.exclude = excludeKeys;
    }

    public DynamicPropertiesSchema(Set<String> excludeKeys, JSONObject config) {
        this.exclude = excludeKeys;
        JSONArray include = config.optJSONArray("include");
        if(include != null) this.include = ConversionUtils.toSet(include);
        JSONArray exclude = config.optJSONArray("exclude");
        if(exclude != null) this.exclude.addAll(ConversionUtils.<String>toSet(exclude));
    }


    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        if(include == null && exclude == null) return source;
        HashMap<String, Object> results = new HashMap<>();
        source.entrySet().stream()
                .filter(prop -> filter(prop.getKey()))
                .forEach(prop -> results.put(prop.getKey(), prop.getValue()));
        return results;
    }

    private boolean filter(String key) {
        return (include == null || include.contains(key)) && (exclude == null || !exclude.contains(key));
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> properties) {
        return properties;
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        return predicatesHolder;
    }
}
