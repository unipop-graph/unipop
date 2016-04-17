package org.unipop.common.property;

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.javatuples.Pair;
import org.json.JSONObject;

import java.util.Iterator;
import java.util.Map;

public class FieldPropertySchema implements PropertySchema {
    private String key;
    private String field = null;
    private String[] include = new String[0];
    private String[] exclude = new String[0];

    public FieldPropertySchema(String key, String field) {
        this.key = key;
        this.field = field;
    }
    
    public FieldPropertySchema(String key, Object config) {
        this.key = key;
        if(config instanceof JSONObject){
            JSONObject configurationMap = (JSONObject) config;
            configurationMap.keys().forEachRemaining(configKey -> {
                Object configValue = configurationMap.get(configKey);
                if(configKey.equals("include")) this.include = getArray(configValue);
                else if(configKey.equals("exclude")) this.exclude = getArray(configValue);
                else if(configKey.equals("field")) this.field = configValue.toString();
            });
        }

    }

    private String[] getArray(Object value) {
        if(value instanceof String[]) return (String[]) value;
        return new String[]{ value.toString() };
    }

    @Override
    public Pair<String, Object> toProperty(Map<String, Object> source) {
        Object value = source.get(this.field);
        if(value != null) return Pair.with(this.key, value);
        return null;
    }

    @Override
    public Iterator<Pair<String, Object>> toFields(Object prop) {
        return Iterators.singletonIterator(Pair.with(this.key, prop));
    }

    @Override
    public boolean test(P predicate) {
        boolean included = this.include.length == 0;
        for(String include : this.include) {
            included |= predicate.test(include);
        }
        boolean excluded = this.exclude.length > 0;
        for(String exclude : this.exclude) {
            excluded |= predicate.test(exclude);
        }

        return included && !excluded;
    }
}
