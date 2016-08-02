package org.unipop.schema.property;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class ConcatenateFieldPropertySchema implements PropertySchema {
    private final String key;
    private final List<PropertySchema> schemas;
    private String delimiter;

    public ConcatenateFieldPropertySchema(String key, List<PropertySchema> schemas, String delimiter, boolean nullable) {
        this.key = key;
        this.schemas = schemas;
        this.delimiter = delimiter;
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        StringJoiner values = new StringJoiner(delimiter);
        for (PropertySchema schema : schemas) {
            schema.toProperties(source).values().stream().map(Object::toString).forEach(values::add);
        }
        return Collections.singletonMap(key, values.toString());
    }

    @Override
    public Set<String> excludeDynamicFields() {
        return schemas.stream()
                .map(PropertySchema::excludeDynamicFields)
                .flatMap(Collection::stream).collect(Collectors.toSet());
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
        return propertyKeys.contains(key) ? schemas.stream()
                .flatMap(s -> s.toFields(propertyKeys).stream()).collect(Collectors.toSet()) :
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

    private PredicatesHolder stringValueToPredicate(String value, HasContainer has, boolean collection) {
        String[] values = value.split(delimiter);
        if (values.length < schemas.size()) return PredicatesHolderFactory.abort();
        Set<PredicatesHolder> predicates = new HashSet<>();
        for (int i = 0; i < schemas.size(); i++) {
            P predicate = has.getPredicate().clone();
            final Object currentValue = values[i];
            P p = new P(predicate.getBiPredicate(), collection ? Arrays.asList(currentValue) : currentValue);
            PredicatesHolder predicatesHolder = schemas.get(i).toPredicates(PredicatesHolderFactory.predicate(new HasContainer(has.getKey(), p)));
            predicates.add(predicatesHolder);
        }
        return PredicatesHolderFactory.and(predicates);
    }

    private PredicatesHolder toPredicate(HasContainer has) {
        Object value = has.getValue();
        Set<PredicatesHolder> predicates = new HashSet<>();
        if (value instanceof String) {
            String valueString = value.toString();
            predicates.add(stringValueToPredicate(valueString, has, false));
        } else if (value instanceof Collection) {
            Collection collection = (Collection) value;
            collection.forEach(v -> predicates.add(stringValueToPredicate(v.toString(), has, true)));
            Map<String, List<HasContainer>> collect = predicates.stream().flatMap(p -> p.getPredicates().stream()).collect(Collectors.groupingBy(p -> p.getKey()));
            predicates.clear();
            collect.forEach((key, hasContainers) -> {
                List<Object> values = hasContainers.stream().map(HasContainer::getValue)
                        .map(l -> ((Collection) l).iterator().next()).collect(Collectors.toList());
                predicates.add(PredicatesHolderFactory.predicate(new HasContainer(key,
                        new P(has.getBiPredicate(), values))));
            });
        }
        return PredicatesHolderFactory.and(predicates);
    }

    public static class Builder implements PropertySchemaBuilder {
        @Override
        public PropertySchema build(String key, Object conf) {
            if (!(conf instanceof JSONObject)) return null;
            JSONObject config = (JSONObject) conf;
            Object obj = config.opt("field");
            String delimiter = config.optString("delimiter", "_");
            if (obj == null || !(obj instanceof JSONArray)) return null;
            JSONArray fieldsArray = (JSONArray) obj;
            List<PropertySchema> schemas = new ArrayList<>();
            for (int i = 0; i < fieldsArray.length(); i++) {
                Object field = fieldsArray.get(i);
                schemas.add(AbstractPropertyContainer.createPropertySchema(key, field, true));
            }
            return new ConcatenateFieldPropertySchema(key, schemas, delimiter, config.optBoolean("nullable", true));
        }
    }
}
