package org.unipop.elastic.document.schema.property;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.json.JSONObject;
import org.unipop.schema.property.AbstractPropertyContainer;
import org.unipop.schema.property.FieldPropertySchema;
import org.unipop.schema.property.PropertySchema;

import java.util.Collections;
import java.util.Map;

public class KeywordPropertySchema extends FieldPropertySchema {
    public KeywordPropertySchema(String key, String field, boolean nullable) {
        super(key, field, nullable);
    }

    public KeywordPropertySchema(String key, JSONObject config, boolean nullable) {
        super(key, config, nullable);
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> properties) {
        Object value = properties.get(this.key);
        if (value == null && nullable) return Collections.emptyMap();
        if (value == null || !test(P.eq(value))) return null;
        return Collections.singletonMap(this.field + ".keyword", value);
    }

    public static class Builder implements PropertySchemaBuilder {
        @Override
        public PropertySchema build(String key, Object conf, AbstractPropertyContainer container) {
            boolean nullable = !(key.equals("~id") || key.equals("~label"));
            if (!(conf instanceof JSONObject)) return null;
            JSONObject config = (JSONObject) conf;
            Object field = config.opt("field");
            if (!(config.optBoolean("keyword")) || field == null) return null;
            return new KeywordPropertySchema(key, config, config.optBoolean("nullable", nullable));
        }
    }
}
