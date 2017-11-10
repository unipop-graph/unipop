package org.unipop.schema.property;

import org.json.JSONObject;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.util.PropertySchemaFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MetaPropertySchema implements ParentSchemaProperty {
    private PropertySchema mainSchema;
    private List<PropertySchema> propertySchemas;
    private String key;

    private MetaPropertySchema(String key, PropertySchema mainSchema, List<PropertySchema> propertySchemas) {
        this.key = key;
        this.mainSchema = mainSchema;
        this.propertySchemas = propertySchemas;
    }

    @Override
    public Collection<PropertySchema> getChildren() {
        return Stream.of(propertySchemas, Collections.singletonList(mainSchema))
                .flatMap(Collection::stream).collect(Collectors.toList());
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        Map<String, Object> map = new HashMap<>();
        map.put("value", mainSchema.toProperties(source));
        Map<String, Object> props = new HashMap<>();
        propertySchemas.forEach(schema -> props.putAll(schema.toProperties(source)));
        map.put("properties", props);
        return Collections.singletonMap(key, map);
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> properties) {
        Map<String, Object> fields = new HashMap<>();
        getChildren().forEach(schema -> fields.putAll(schema.toFields(properties)));
        return fields;
    }

    @Override
    public Set<Object> getValues(PredicatesHolder predicatesHolder) {
        return null;
    }

    public static class Builder implements PropertySchemaBuilder {

        @Override
        public PropertySchema build(String key, Object conf, AbstractPropertyContainer container) {
            if (!(conf instanceof JSONObject)) return null;
            if (!((JSONObject) conf).has("properties")) return null;
            PropertySchema mainSchema = PropertySchemaFactory.createPropertySchema(key, ((JSONObject) conf).opt("schema"), container);
            JSONObject properties = ((JSONObject) conf).optJSONObject("properties");
            List<PropertySchema> propertySchemas = new ArrayList<>();
            properties.keys().forEachRemaining(prop -> {
                propertySchemas.add(PropertySchemaFactory.createPropertySchema(prop, properties.opt(prop), container));
            });
            return new MetaPropertySchema(key, mainSchema, propertySchemas);
        }
    }
}
