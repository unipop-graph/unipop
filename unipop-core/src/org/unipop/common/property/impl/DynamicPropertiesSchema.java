package org.unipop.common.property.impl;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.unipop.common.property.PropertySchema;
import org.unipop.common.util.ConversionUtils;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.util.*;
import java.util.stream.Collectors;

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
        if(exclude != null) this.exclude.addAll(ConversionUtils.toSet(exclude));
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        return source.entrySet().stream().filter(prop -> include(prop.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getKey));
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> properties) {
        return properties.entrySet().stream().filter(entry -> include(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getKey));
    }

    @Override
    public PredicatesHolder toPredicates(HasContainer has) {
        if(!include(has.getKey())) return PredicatesHolderFactory.predicate(has);
        return null;
    }

    private boolean include(String key) {
        return (include == null || include.size() == 0 || include.contains(key)) &&
                (exclude == null || exclude.size() == 0 && !exclude.contains(key));
    }
}
