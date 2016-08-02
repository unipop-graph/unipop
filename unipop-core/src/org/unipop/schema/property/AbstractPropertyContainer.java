package org.unipop.schema.property;

import org.apache.tinkerpop.gremlin.structure.T;
import org.json.JSONArray;
import org.json.JSONObject;
import org.unipop.structure.UniGraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public abstract class AbstractPropertyContainer {
    protected final UniGraph graph;
    protected final JSONObject json;
    protected ArrayList<PropertySchema> propertySchemas = new ArrayList<>();
    protected DynamicPropertySchema dynamicProperties;
    public static List<PropertySchema.PropertySchemaBuilder> builders = new ArrayList<>();

    public AbstractPropertyContainer(JSONObject json, UniGraph graph) {
        this.json = json;
        this.graph = graph;
        builders.add(new StaticPropertySchema.Builder());
        builders.add(new FieldPropertySchema.Builder());
        builders.add(new DateFieldPropertySchema.Builder());
        builders.add(new StaticDatePropertySchema.Builder());
        builders.add(new MultiFieldPropertySchema.Builder());
        builders.add(new ConcatenateFieldPropertySchema.Builder());
        Collections.reverse(builders);
        createPropertySchemas();
    }

    protected List<PropertySchema> getPropertySchemas() {
        return propertySchemas;
    }

    protected void createPropertySchemas() {
        addPropertySchema(T.id.getAccessor(), json.get(T.id.toString()), false);
        addPropertySchema(T.label.getAccessor(), json.get(T.label.toString()), false);

        JSONObject properties = json.optJSONObject("properties");
        if (properties != null) {
            properties.keys().forEachRemaining(key -> addPropertySchema(key, properties.get(key), true));
        }

        Object dynamicPropertiesConfig = json.opt("dynamicProperties");
        if (dynamicPropertiesConfig instanceof Boolean && (boolean) dynamicPropertiesConfig)
            this.dynamicProperties = new DynamicPropertySchema(propertySchemas);
        else if (dynamicPropertiesConfig instanceof JSONObject)
            this.dynamicProperties = new DynamicPropertySchema(propertySchemas, (JSONObject) dynamicPropertiesConfig);
        else this.dynamicProperties = new NonDynamicPropertySchema(propertySchemas);

        propertySchemas.add(this.dynamicProperties);
    }

    protected void addPropertySchema(String key, Object value, boolean nullable) {
        PropertySchema propertySchema = createPropertySchema(key, value, nullable);
        propertySchemas.add(propertySchema);
    }


    @Override
    public String toString() {
        return "AbstractPropertyContainer{" +
                "dynamicProperties=" + dynamicProperties +
                ", graph=" + graph +
                ", propertySchemas=" + propertySchemas +
                '}';
    }

    protected static PropertySchema createPropertySchema(String key, Object value, boolean nullable) {
        Optional<PropertySchema> first = builders.stream().map(builder -> builder.build(key, value)).filter(schema -> schema != null).findFirst();
        if (first.isPresent()) return first.get();
        throw new IllegalArgumentException("Unrecognized property: " + key + " - " + value);
    }
}
