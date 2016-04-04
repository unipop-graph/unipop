package org.unipop.schema.impl;

import org.apache.commons.configuration.ConfigurationMap;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class BasicSchema {

    protected final PropertySchema id;
    protected final PropertySchema label;
    protected final Map<String, PropertySchema> properties = new HashMap<>();
    public boolean dynamicProperties;

    public BasicSchema(HierarchicalConfiguration configuration) {
        this.id = createPropertySchema("id", configuration.getProperty("id"));
        this.label = createPropertySchema("label", configuration.getProperty("label"));
        ConfigurationMap properties = new ConfigurationMap(configuration.subset("properties"));
        properties.forEach((key, value) -> this.properties.put(key.toString(), createPropertySchema(key.toString(), value)));
        this.dynamicProperties = configuration.getBoolean("dynamicProperties", true);
    }


    protected PropertySchema createPropertySchema(String key, Object value) {
        return new PropertySchema(key, value);
    }

    protected String getLabel(Map<String, Object> keyValues) {
        return this.label.popValue(keyValues).toString();
    }

    protected Object getId(Map<String, Object> keyValues) {
        return this.id.popValue(keyValues);
    }

    protected Map<String, Object> getProperties(Map<String, Object> keyValues) {
        HashMap<String, Object> sourceClone = new HashMap<>(keyValues);

        Map<String, Object> results = new HashMap<>();
        properties.forEach((key, property) -> {
            Object value = property.popValue(sourceClone);
            if(value != null) results.put(key, value);
        });

        if(this.dynamicProperties) {
            sourceClone.forEach((key, value) -> results.merge(key, value, this::mergeProperties));
            results.putAll(sourceClone);
        }

        return results;
    }

    protected Object mergeProperties(Object prop1, Object prop2) {
        return prop1;
    }

    protected Map<String, Object> toFields(Object id, String label, Iterator<? extends Property<Object>> properties) {
        Map<String, Object> fields = new HashMap<>();
        addFields(fields, this.id.toFields(id));
        addFields(fields, this.label.toFields(label));

        properties.forEachRemaining(property -> {
            PropertySchema propertySchema = this.properties.get(property.key());
            if(propertySchema != null) addFields(fields, propertySchema.toFields(property.value()));
        });

        return fields;
    }

    private void addFields(Map<String, Object> fields, Map<String, Object> stringObjectMap) {
        stringObjectMap.forEach((key, value) -> fields.merge(key, value, this::mergeFields));
    }

    protected Object mergeFields(Object obj1, Object obj2) {
        return obj1;
    }
}
