package org.unipop.schema.property;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.schema.property.type.PropertyType;
import org.unipop.util.PropertySchemaFactory;

import java.util.*;
import java.util.stream.Collectors;


public class ConcatenateFieldPropertySchema implements ParentSchemaProperty {
    private final String key;
    private final List<PropertySchema> schemas;
    private String delimiter;

    public ConcatenateFieldPropertySchema(String key, List<PropertySchema> schemas, String delimiter, boolean nullable) {
        this.key = key;
        this.schemas = schemas;
        this.delimiter = delimiter;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        StringJoiner values = new StringJoiner(delimiter);
        for (PropertySchema schema : schemas) {
            Map<String, Object> props = schema.toProperties(source);
            if (props == null) values.add("null");
            else if (props.size() == 0) return Collections.emptyMap();
            else props.values().stream().map(Object::toString).forEach(values::add);
        }
        return Collections.singletonMap(key, values.toString());
    }

    @Override
    public Collection<PropertySchema> getChildren() {
        return schemas;
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
        return schemas.stream().flatMap(s -> s.toFields(propertyKeys).stream()).collect(Collectors.toSet());
    }

    @Override
    public Set<Object> getValues(PredicatesHolder predicatesHolder) {
        StringJoiner values = new StringJoiner(delimiter);
        for (PropertySchema schema : schemas) {
            Set<Object> schemaValues = schema.getValues(predicatesHolder);
            if (schemaValues == null || schemaValues.size() == 0) return Collections.emptySet();
            schemaValues.forEach(value -> values.add(value.toString()));
        }
        return Collections.singleton(values.toString());
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

    @Override
    public PredicatesHolder toPredicate(HasContainer has) {
        Object value = has.getValue();
        Set<PredicatesHolder> predicates = new HashSet<>();
        if (value instanceof String) {
            String valueString = value.toString();
            predicates.add(stringValueToPredicate(valueString, has, false));
        } else if (value instanceof Collection) {
            Collection collection = (Collection) value;
            collection.forEach(v -> predicates.add(stringValueToPredicate(v.toString(), has, true)));
            Map<String, List<HasContainer>> collect = predicates.stream().flatMap(p -> p.getPredicates().stream()).collect(Collectors.groupingBy(p -> p.getKey()));
            if (collect.size() == 0) return PredicatesHolderFactory.abort();
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
        public PropertySchema build(String key, Object conf, AbstractPropertyContainer container) {
            if (!(conf instanceof JSONObject)) return null;
            JSONObject config = (JSONObject) conf;
            Object obj = config.opt("fields");
            String delimiter = config.optString("delimiter", "_");
            if (obj == null || !(obj instanceof JSONArray)) return null;
            JSONArray fieldsArray = (JSONArray) obj;
            List<PropertySchema> schemas = new ArrayList<>();
            for (int i = 0; i < fieldsArray.length(); i++) {
                Object field = fieldsArray.get(i);
                schemas.add(PropertySchemaFactory.createPropertySchema(key, field, container));
            }
            return new ConcatenateFieldPropertySchema(key, schemas, delimiter, config.optBoolean("nullable", true));
        }
    }
}
