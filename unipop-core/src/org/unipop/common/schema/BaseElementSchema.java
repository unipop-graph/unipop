package org.unipop.common.schema;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.common.schema.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.util.HashMap;
import java.util.List;
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
            Object value = property.toProperty(sourceClone);
            if(value != null) properties.put(key, value);
        });

        if(this.dynamicProperties) {
            sourceClone.forEach((key, value) -> properties.merge(key, value, this::mergeFromFields));
            properties.putAll(sourceClone);
        }
        return properties;
    }

    protected Object mergeFromFields(Object prop1, Object prop2) {
        return prop1;
    }

    @Override
    public Map<String, Object> toFields(E element) {
        Map<String, Object> properties = fullProperties(element);

        Map<String, Object> fields = new HashMap<>();
        properties.forEach((key, value) -> {
            PropertySchema propertySchema = this.properties.get(key);
            if(propertySchema != null) {
                propertySchema.toFields(value).forEachRemaining(pair ->
                        fields.merge(pair.getValue0(), pair.getValue1(), this::mergeToFields));
            }
        });

        return fields;
    }

    protected Map<String, Object> fullProperties(E element) {

    }

    protected Object mergeToFields(Object obj1, Object obj2) {
        return obj1;
    }

    @Override
    public List<HasContainer> toPredicates(List<HasContainer> predicates) {

    }
}
