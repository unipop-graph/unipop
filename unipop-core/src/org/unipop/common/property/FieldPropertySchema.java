package org.unipop.common.property;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
        Object prop = properties.get(this.key);
        if(prop == null || !test(P.eq(prop))) return null;
        return Collections.singletonMap(this.field, prop);
    }

    @Override
    public boolean test(P predicate) {
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
