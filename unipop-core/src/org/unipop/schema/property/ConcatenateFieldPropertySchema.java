package org.unipop.schema.property;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class ConcatenateFieldPropertySchema implements PropertySchema {
    private final String key;
    private final List<String> fields;
    private String delimiter;

    public ConcatenateFieldPropertySchema(String key, List<String> fields, String delimiter, boolean nullable) {
        this.key = key;
        this.fields = fields;
        this.delimiter = delimiter;
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        String finalValue = null;
        for (String field : fields) {
            Object value;
            if (field.startsWith("@"))
                value = source.get(field.substring(1));
            else
                value = field;
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
        return propertyKeys.contains(key) ? fields.stream()
                .filter(s -> s.startsWith("@")).map(s -> s.substring(1)).collect(Collectors.toSet()) :
                Collections.emptySet();
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
                if (keys[i].startsWith("@"))
                    predicates.add(new HasContainer(keys[i].substring(1), p));
                else if (!p.test(keys[i]))
                    return PredicatesHolderFactory.abort();
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
            final boolean[] abort = {false};
            predicatesValues.entrySet().forEach(kv -> {
                if (kv.getKey().startsWith("@"))
                    predicates.add(new HasContainer(kv.getKey().substring(1), new P(has.getBiPredicate(), kv.getValue())));
                else if (!new P(has.getBiPredicate(), kv.getValue()).test(kv.getKey())) {
                    abort[0] = true;
                    return;
                }
            });
            if (abort[0])
                return PredicatesHolderFactory.abort();
        }
        return PredicatesHolderFactory.and(predicates.toArray(new HasContainer[predicates.size()]));
    }
}
