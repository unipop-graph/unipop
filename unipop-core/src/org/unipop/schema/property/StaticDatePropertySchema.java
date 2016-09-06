package org.unipop.schema.property;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.json.JSONObject;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

/**
 * Created by sbarzilay on 8/2/16.
 */
public class StaticDatePropertySchema extends StaticPropertySchema implements DatePropertySchema {
    protected final String sourceFormat;
    protected final String displayFormat;

    public StaticDatePropertySchema(String key, String value, JSONObject config) {
        super(key, value);
        this.sourceFormat = config.optString("sourceFormat");
        this.displayFormat = config.optString("displayFormat", "yyyy-MM-dd HH:mm:ss:SSS");
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        Date date = fromSource(this.value);
        return Collections.singletonMap(this.key, toDisplay(date));
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> prop) {
        return Collections.emptyMap();
    }

    @Override
    protected boolean test(P predicate) {
        return predicate.test(toDisplay(fromSource(this.value)));
    }

    @Override
    public DateFormat getSourceDateFormat() {
        return new SimpleDateFormat(sourceFormat);
    }

    @Override
    public DateFormat getDisplayDateFormat() {
        return new SimpleDateFormat(displayFormat);
    }

    public static class Builder implements PropertySchemaBuilder {
        @Override
        public PropertySchema build(String key, Object conf, AbstractPropertyContainer container) {
            if (!(conf instanceof JSONObject)) return null;
            JSONObject config = (JSONObject) conf;
            Object value = config.opt("value");
            Object format = config.opt("sourceFormat");
            if (value == null || format == null) return null;
            return new StaticDatePropertySchema(key, value.toString(), config);

        }
    }
}
