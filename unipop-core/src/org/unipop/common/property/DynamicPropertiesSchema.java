package org.unipop.common.property;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

public class DynamicPropertiesSchema implements PropertySchema {

    private Set<? extends Object> include;
    private Set<? extends Object> exclude;

    public DynamicPropertiesSchema() {

    }

    public DynamicPropertiesSchema(JSONObject config) {
        JSONArray include = config.optJSONArray("include");
        if(include != null) this.include = toSet(include);
        JSONArray exclude = config.optJSONArray("exclude");
        if(exclude != null) this.exclude = toSet(exclude);
    }

    private Set toSet(JSONArray jsonArray) {
        HashSet hashSet = new HashSet(jsonArray.length());
        for(int i = 0; i < jsonArray.length(); i++)
            hashSet.add(jsonArray.get(i));
        return hashSet;
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        if(include == null && exclude == null) return source;
        HashMap<String, Object> results = new HashMap<>();
        source.entrySet().stream()
                .filter(prop -> (include == null || include.contains(prop.getKey())) && (exclude == null || !exclude.contains(prop.getKey())))
                .forEach(prop -> results.put(prop.getKey(), prop.getValue()));
        return results;
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> properties) {
        return properties;
    }
}
