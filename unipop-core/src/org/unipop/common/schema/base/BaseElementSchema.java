package org.unipop.common.schema.base;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.javatuples.Pair;
import org.unipop.common.schema.ElementSchema;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.common.property.PropertySchema;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;

import java.util.HashMap;
import java.util.Map;

public abstract class BaseElementSchema<E extends Element> implements ElementSchema<E> {

    protected final Map<String, PropertySchema> properties;
    protected final boolean dynamicProperties;
    protected UniGraph graph;

    public BaseElementSchema(Map<String, PropertySchema> properties, boolean dynamicProperties, UniGraph graph) {
        this.properties = properties;
        this.dynamicProperties = dynamicProperties;
        this.graph = graph;
    }

    protected Map<String, Object> getProperties(Map<String, Object> keyValues) {
        HashMap<String, Object> sourceClone = new HashMap<>(keyValues);

        Map<String, Object> properties = new HashMap<>();
        this.properties.forEach((key, property) -> {
            Pair<String, Object> value = property.toProperty(sourceClone);
            if(value != null) properties.put(value.getValue0(), value.getValue1());
        });

        if(this.dynamicProperties) {
            sourceClone.forEach((key, value) -> properties.merge(key, value, this::mergeProperties));
            properties.putAll(sourceClone);
        }
        return properties;
    }

    protected Object mergeProperties(Object prop1, Object prop2) {
        return prop1;
    }

    @Override
    public Map<String, Object> toFields(E element) {
        Map<String, Object> properties = UniElement.fullProperties(element);
        assert properties != null;

        Map<String, Object> fields = new HashMap<>();
        properties.forEach((key, value) -> {
            PropertySchema propertySchema = this.properties.get(key);
            if(propertySchema != null) {
                propertySchema.toFields(value).forEachRemaining(pair ->
                        fields.merge(pair.getValue0(), pair.getValue1(), this::mergeFields));
            }
        });

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
            else propertySchema.toFields(has.getValue()).forEachRemaining(field -> {
                HasContainer newHas= new HasContainer(field.getValue0(), new P(has.getBiPredicate(), field.getValue1()));
                newPredicates.add(newHas);
            });
        }

        predicatesHolder.getChildren().forEach(child -> newPredicates.add(this.toPredicates(child)));
        if(newPredicates.isEmpty()) return null;
        return newPredicates;
    }
}
