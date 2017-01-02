package org.unipop.elastic.document.schema.property;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.json.JSONObject;
import org.unipop.schema.property.AbstractPropertyContainer;
import org.unipop.schema.property.FieldPropertySchema;
import org.unipop.schema.property.PropertySchema;

import java.util.Collections;
import java.util.Map;

/**
 * Created by sbarzilay on 14/12/16.
 */
public class InnerPropertySchema extends FieldPropertySchema {
    public InnerPropertySchema(String key, String field, boolean nullable) {
        super(key, field, nullable);
    }

    public InnerPropertySchema(String key, JSONObject config, boolean nullable) {
        super(key, config, nullable);
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        String[] path = this.field.split("\\.");
        Object value = source;
        for (String s : path) {
            if (value == null && nullable) return Collections.emptyMap();
            if (value == null) return null;
            value = ((Map<String, Object>) value).get(s);
        }
        if (value == null && nullable) return Collections.emptyMap();
        if (value == null || !test(P.eq(value))) return null;
        return Collections.singletonMap(this.key, value);
    }

    public static class Builder implements PropertySchemaBuilder {
        @Override
        public PropertySchema build(String key, Object conf, AbstractPropertyContainer container) {
            boolean nullable = !(key.equals("~id") || key.equals("~label"));
            if (conf instanceof String){
                String field = conf.toString();
                if (!field.startsWith("@") || !field.contains(".")) return null;
                return new InnerPropertySchema(key, field.substring(1), nullable);
            }
            if (!(conf instanceof JSONObject)) return null;
            JSONObject config = (JSONObject) conf;
            Object field = config.opt("field");
            if (field == null || !field.toString().contains(".")) return null;
            return new InnerPropertySchema(key, config, config.optBoolean("nullable", nullable));
        }
    }
}
