package org.unipop.common.schema.base;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.common.schema.ElementSchema;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.common.property.PropertySchema;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class BaseElementSchema<E extends Element> implements ElementSchema<E> {

    protected final Map<String, PropertySchema> properties;
    protected final PropertySchema dynamicProperties;
    protected UniGraph graph;

    public BaseElementSchema(Map<String, PropertySchema> properties, PropertySchema dynamicProperties, UniGraph graph) {
        this.properties = properties;
        this.dynamicProperties = dynamicProperties;
        this.graph = graph;
    }

    protected Map<String, Object> getProperties(Map<String, Object> source) {
        Map<String, Object> result = new HashMap<>();
        this.properties.forEach((key, schema) -> {
            Map<String, Object> schemaProperties = schema.toProperties(source);
            if(schemaProperties != null)
                schemaProperties.forEach((propKey, propValue) -> result.merge(propKey, propValue, this::mergeProperties));
        });

        if(this.dynamicProperties != null) {
            Map<String, Object> properties = dynamicProperties.toProperties(source);
            properties.forEach((key, value) -> result.merge(key, value, this::mergeProperties));
        }
        return result;
    }

    protected Object mergeProperties(Object prop1, Object prop2) {
        return prop1;
    }

    @Override
    public Map<String, Object> toFields(E element) {
        Map<String, Object> properties = UniElement.fullProperties(element);
        assert properties != null;

        Map<String, Object> fields = new HashMap<>();
        this.properties.forEach((key, schema) -> {
            Map<String, Object> schemaFields = schema.toFields(properties);
            if(schemaFields != null)
                schemaFields.forEach((fieldKey, fieldValue) -> fields.merge(fieldKey, fieldValue, this::mergeFields));
        });
        if(this.dynamicProperties != null) {
            Map<String, Object> dynamicFields = dynamicProperties.toFields(properties);
            dynamicFields.forEach((key, value) -> fields.merge(key, value, this::mergeProperties));
        }

        return fields;
    }

    protected Object mergeFields(Object obj1, Object obj2) {
        return obj1;
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        if(predicatesHolder.isEmpty()) return predicatesHolder;
        PredicatesHolder newPredicates = new PredicatesHolder(predicatesHolder.getClause());

        for(HasContainer has : predicatesHolder.getPredicates()) {
            PropertySchema propertySchema = properties.get(has.getKey());
            if(propertySchema == null) continue;
            if(!propertySchema.test(has.getPredicate())) {
                if(predicatesHolder.getClause().equals(PredicatesHolder.Clause.And)) return null;
            }
            else {
                Map<String, Object> fields = propertySchema.toFields(Collections.singletonMap(has.getKey(), has.getValue()));
                fields.forEach((fieldKey, fieldValue) -> {
                    HasContainer newHas = new HasContainer(fieldKey, new P(has.getBiPredicate(), fieldValue));
                    newPredicates.add(newHas);
                });
            }
        }

        predicatesHolder.getChildren().forEach(child -> newPredicates.add(this.toPredicates(child)));
        if(newPredicates.isEmpty()) return null;
        return newPredicates;
    }
}
