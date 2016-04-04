package org.unipop.schema.impl;

import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.Map;

/**
 * Created by ranma on 29/03/2016.
 */
public class PropertySchema {
    private final String value;
    private Object key;

    public PropertySchema(String key, Object configurationProperty) {
        if(configurationProperty instanceof String){
            this.value = configurationProperty.toString();
        } else if(configurationProperty instanceof Map){

        }

    }

    public Object popValue(Map<String, Object> source) {

    }

    public Map<String, Object> toFields(Object prop) {
    }

    public boolean test(Object value, P<?> predicate) {
        return true;
    }
}
