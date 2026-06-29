package org.unipop.schema.property;

import org.apache.tinkerpop.gremlin.structure.T;
import org.json.JSONArray;
import org.json.JSONObject;
import org.unipop.structure.UniGraph;
import org.unipop.util.PropertySchemaFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractPropertyContainer {
    protected final UniGraph graph;
    protected final JSONObject json;
    protected ArrayList<PropertySchema> propertySchemas = new ArrayList<>();
    protected DynamicPropertySchema dynamicProperties;

    public AbstractPropertyContainer(JSONObject json, UniGraph graph) {
        this.json = json;
        this.graph = graph;
        createPropertySchemas();
    }

    public List<PropertySchema> getPropertySchemas() {
        return propertySchemas;
    }

    protected void createPropertySchemas() {
        addPropertySchema(T.id.getAccessor(), json.get(T.id.toString()));
        addPropertySchema(T.label.getAccessor(), json.get(T.label.toString()));

        JSONObject properties = json.optJSONObject("properties");
        if (properties != null) {
            properties.keys().forEachRemaining(key -> addPropertySchema(key, properties.get(key)));
        }

        Object jsonbColumnsConfig = json.opt("jsonbColumns");
        Object dynamicPropertiesConfig = json.opt("dynamicProperties");
        if (jsonbColumnsConfig instanceof JSONArray)
            this.dynamicProperties = new MultiJsonbPropertySchema(propertySchemas, (JSONArray) jsonbColumnsConfig);
        else if (dynamicPropertiesConfig instanceof Boolean && (boolean) dynamicPropertiesConfig)
            this.dynamicProperties = new DynamicPropertySchema(propertySchemas);
        else if (dynamicPropertiesConfig instanceof JSONObject) {
            JSONObject dpc = (JSONObject) dynamicPropertiesConfig;
            this.dynamicProperties = dpc.has("jsonb")
                    ? new JsonbPropertySchema(propertySchemas, dpc)
                    : new DynamicPropertySchema(propertySchemas, dpc);
        }
        else this.dynamicProperties = new NonDynamicPropertySchema(propertySchemas);

        propertySchemas.add(this.dynamicProperties);
    }

    protected void addPropertySchema(String key, Object value) {
        PropertySchema propertySchema = PropertySchemaFactory.createPropertySchema(key, value, this);
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
}
