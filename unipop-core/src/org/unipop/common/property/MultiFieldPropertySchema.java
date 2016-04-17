package org.unipop.common.property;

import org.javatuples.Pair;

import java.util.*;
import java.util.stream.Collectors;


public class MultiFieldPropertySchema implements PropertySchema {
    private final String key;
    private final List<String> fields;
    private String delimiter;

    public MultiFieldPropertySchema(String key, String[] fields, String delimiter) {
        this.key = key;
        this.fields = Arrays.asList(fields);
        this.delimiter = delimiter;
    }

    @Override
    public Pair<String, Object> toProperty(Map<String, Object> source) {
        String value = fields.stream().map(field -> source.get(field).toString()).collect(Collectors.joining(delimiter));
        if (value != null && value.length() > 0)
            return Pair.with(key, value);
        else return null;
    }

    @Override
    public Iterator<Pair<String, Object>> toFields(Object prop) {
        List<Pair<String, Object>> result = new ArrayList<>(fields.size());
        String[] values = prop.toString().split(delimiter);
        //TODO: what if values.length != fields.length ??? o_O
        for(int i = 0; i < fields.size(); i++) {
            result.add(Pair.with(fields.get(i), values[i]));
        }
        return result.iterator();
    }
}
