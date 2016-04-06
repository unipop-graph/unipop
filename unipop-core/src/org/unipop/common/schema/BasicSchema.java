package org.unipop.common.schema;

import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.configuration.ConfigurationMap;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.unipop.common.property.FieldPropertySchema;
import org.unipop.common.property.MultiFieldPropertySchema;
import org.unipop.common.property.PropertySchema;
import org.unipop.common.property.StaticPropertySchema;
import org.unipop.common.util.StreamUtils;
import org.unipop.structure.UniGraph;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class BasicSchema<E extends Element> implements ElementSchema<E> {

    protected final UniGraph graph;
    protected final Map<String, PropertySchema> properties = new HashMap<>();
    protected boolean dynamicProperties;

    public BasicSchema(HierarchicalConfiguration configuration, UniGraph graph) throws MissingArgumentException {
        this.graph = graph;
        addPropertySchema(T.id.getAccessor(), configuration.getProperty(T.id.getAccessor()));
        addPropertySchema(T.label.getAccessor(), configuration.getProperty(T.label.getAccessor()));
        ConfigurationMap properties = new ConfigurationMap(configuration.subset("properties"));
        for(Map.Entry<Object, Object> property : properties.entrySet()) {
            addPropertySchema(property.getKey().toString(), property.getValue());
        }
        this.dynamicProperties = configuration.getBoolean("dynamicProperties", true);
    }

    protected void addPropertySchema(String key, Object value) throws MissingArgumentException {
        PropertySchema propertySchema = createPropertySchema(key, value);
        properties.put(key, propertySchema);
    }

    protected PropertySchema createPropertySchema(String key, Object value) throws MissingArgumentException {
        if(value instanceof String) {
            if (key.startsWith("@"))
                return new FieldPropertySchema(key, value.toString().substring(1));
            else return new StaticPropertySchema(key, value.toString());
        }
        else if(value instanceof Map) {
            Map<String, Object> config = (Map<String, Object>) value;
            Object field = config.get("field");
            if(field != null && field instanceof String)
                return new FieldPropertySchema(key, config);
            else if(field != null && field instanceof String[])
                return new MultiFieldPropertySchema(key, (String[])field, "_");
        }
        throw new MissingArgumentException("");
    }


    @Override
    public E fromFields(Map<String, Object> fields) {
        Map<String, Object> stringObjectMap = getProperties(fields);
        return createElement(stringObjectMap);
    }

    protected abstract E createElement(Map<String, Object> properties);

    protected Map<String, Object> getProperties(Map<String, Object> keyValues) {
        HashMap<String, Object> sourceClone = new HashMap<>(keyValues);

        Map<String, Object> results = new HashMap<>();
        properties.forEach((key, property) -> {
            Object value = property.toProperty(sourceClone);
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

    @Override
    public Map<String, Object> toFields(E element) {
        Map<String, Object> elementProperties = StreamUtils.asStream(element.properties()).collect(Collectors.toMap(Property::key, Property::value));
        elementProperties.put(T.id.toString(), element.id());
        elementProperties.put(T.label.toString(), element.label());
        Map<String, Object> fields = new HashMap<>();
        elementProperties.forEach((key, value) -> {
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

}
