package org.unipop.schema.property;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class MultiFieldPropertySchema implements PropertySchema {
    private final String key;
    private final List<String> fields;
    private String delimiter;

    public MultiFieldPropertySchema(String key, List<String> fields, String delimiter, boolean nullable) {
        this.key = key;
        this.fields = fields;
        this.delimiter = delimiter;
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        String finalValue = null;
        for (String field : fields) {
            Object value = source.get(field);
            if (value == null) return Collections.emptyMap();
            finalValue = finalValue == null ? value.toString() : finalValue + delimiter + value.toString();
        }
        return Collections.singletonMap(key, finalValue);
    }

    @Override
    public Set<String> excludeDynamicFields() {
        return new HashSet<>(this.fields);
    }

    @Override
    public Set<String> excludeDynamicProperties() {
        return Collections.singleton(this.key);
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> properties) {
//        Object value = properties.get(this.key);
//        if (value == null) return Collections.emptyMap();
//        Map<String, Object> result = new HashMap<>(fields.size());
//        String[] values = value.toString().split(delimiter);
//            //TODO: what if values.length != fields.length ??? o_O
//            for (int i = 0; i < fields.size(); i++) {
//                result.put(fields.get(i), values[i]);
//            }
//        return result;
        return Collections.emptyMap();
    }

    @Override
    public Set<String> toFields(Set<String> propertyKeys) {
        return propertyKeys.contains(key) ? new HashSet<>(fields) : Collections.emptySet();
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        Stream<HasContainer> hasContainers = predicatesHolder.findKey(this.key);

        Set<PredicatesHolder> predicateHolders = hasContainers.map(this::toPredicate).collect(Collectors.toSet());
        return PredicatesHolderFactory.create(predicatesHolder.getClause(), predicateHolders);
    }

    private void addToList(Map<String, List> map, String key, Object value) {
        if (!map.containsKey(key))
            map.put(key, new ArrayList());
        map.get(key).add(value);
    }

    private PredicatesHolder toPredicate(HasContainer has) {
        String[] keys = fields.toArray(new String[fields.size()]);
        Object value = has.getValue();
        Set<HasContainer> predicates = new HashSet<>();
        if (value instanceof String) {
            String valueString = value.toString();
            String[] values = valueString.split(delimiter);
            for (int i = 0; i < keys.length; i++) {
                P predicate = has.getPredicate().clone();
                final Object currentValue = values[i];
                P p = new P(predicate.getBiPredicate(), currentValue);
                predicates.add(new HasContainer(keys[i], p));
            }
        } else if (value instanceof Collection) {
            Collection values = (Collection) value;
            Map<String, List> predicatesValues = new HashMap<>();
            values.forEach(v -> {
                String[] split = v.toString().split(delimiter);
                for (int i = 0; i < keys.length; i++) {
                    addToList(predicatesValues, keys[i], split[i]);
                }
            });
            predicatesValues.entrySet().forEach(kv -> predicates.add(new HasContainer(kv.getKey(), new P(has.getBiPredicate(), kv.getValue()))));
        }
        return PredicatesHolderFactory.and(predicates.toArray(new HasContainer[predicates.size()]));
    }
}
