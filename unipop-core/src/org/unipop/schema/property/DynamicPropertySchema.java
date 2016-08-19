package org.unipop.schema.property;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.json.JSONObject;
import org.unipop.schema.property.type.PropertyType;
import org.unipop.util.ConversionUtils;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.util.*;
import java.util.stream.Collectors;

public class DynamicPropertySchema implements PropertySchema {

    protected final Set<String> excludeFields;
    protected final Set<String> excludeProperties;

    public DynamicPropertySchema(ArrayList<PropertySchema> otherSchemas) {
        this.excludeFields = otherSchemas.stream().flatMap(schema -> schema.excludeDynamicFields().stream()).collect(Collectors.toSet());
        this.excludeProperties = otherSchemas.stream().flatMap(schema -> schema.excludeDynamicProperties().stream()).collect(Collectors.toSet());
    }

    public DynamicPropertySchema(ArrayList<PropertySchema> otherSchemas, JSONObject config) {
        this(otherSchemas);
        this.excludeFields.addAll(ConversionUtils.toSet(config, "excludeFields"));
        this.excludeProperties.addAll(ConversionUtils.toSet(config, "excludeProperties"));
    }

    @Override
    public String getKey() {
        return null;
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        return source.entrySet().stream().filter(prop -> !excludeFields.contains(prop.getKey()) && prop.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> properties) {
        return properties.entrySet().stream().filter(entry -> !excludeProperties.contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Set<String> toFields(Set<String> propertyKeys) {
        return propertyKeys.stream().filter(key -> !excludeProperties.contains(key))
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Object> getValues(PredicatesHolder predicatesHolder) {
        return null;
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        Set<HasContainer> hasContainers = predicatesHolder.getPredicates().stream().filter(has ->
                !excludeProperties.contains(has.getKey())).collect(Collectors.toSet());

        return PredicatesHolderFactory.createFromPredicates(predicatesHolder.getClause(), hasContainers);
    }

    @Override
    public String toString() {
        return "DynamicPropertySchema{" +
                "excludeFields=" + excludeFields +
                ", excludeProperties=" + excludeProperties +
                '}';
    }
}