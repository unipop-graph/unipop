package org.unipop.schema.property;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.json.JSONArray;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.schema.property.type.PropertyType;
import org.unipop.util.PropertySchemaFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 7/28/16.
 */
public class MultiPropertySchema implements ParentSchemaProperty {
    private String key;
    private List<PropertySchema> schemas;

    public MultiPropertySchema(String key, List<PropertySchema> schemas) {
        this.key = key;
        this.schemas = schemas;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        List<Object> value = new ArrayList<>();
        schemas.forEach(schema -> {
            schema.toProperties(source).values().forEach(value::add);
        });
        return Collections.singletonMap(key, value);
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
        return Collections.emptyMap();
    }

    @Override
    public Set<String> toFields(Set<String> propertyKeys) {
        return schemas.stream().flatMap(s -> s.toFields(propertyKeys).stream()).collect(Collectors.toSet());
    }

    @Override
    public Set<Object> getValues(PredicatesHolder predicatesHolder) {
        return schemas.stream().map(schema -> schema.getValues(predicatesHolder)).flatMap(Collection::stream).collect(Collectors.toSet());
    }

    @Override
    public PredicatesHolder toPredicate(HasContainer hasContainer) {
        Set<PredicatesHolder> predicates = new HashSet<>();
        for (PropertySchema schema : schemas) {
            PredicatesHolder predicatesHolder = schema.toPredicates(PredicatesHolderFactory.predicate(hasContainer));
            if (predicatesHolder.equals(PredicatesHolderFactory.empty()))
                return predicatesHolder;
            if (!predicatesHolder.equals(PredicatesHolderFactory.abort()))
                predicates.add(predicatesHolder);
        }
        return PredicatesHolderFactory.or(predicates);
    }

    public static class Builder implements PropertySchemaBuilder {
        @Override
        public PropertySchema build(String key, Object conf, AbstractPropertyContainer container) {
            if (!(conf instanceof JSONArray)) return null;
            JSONArray fieldsArray = (JSONArray) conf;
            List<PropertySchema> schemas = new ArrayList<>();
            for (int i = 0; i < fieldsArray.length(); i++) {
                schemas.add(PropertySchemaFactory.createPropertySchema(key, fieldsArray.get(i), container));
            }
            return new MultiPropertySchema(key, schemas);
        }
    }
}
