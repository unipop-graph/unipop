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
    protected final SimpleDateFormat sourceFormat;
    protected final SimpleDateFormat displayFormat;

    public StaticDatePropertySchema(String key, String value, JSONObject config) {
        super(key, value);
        this.sourceFormat = new SimpleDateFormat(config.optString("sourceFormat"));
        this.displayFormat = new SimpleDateFormat(config.optString("displayFormat", "yyyy-MM-dd HH:mm:ss:SSS"));
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        Date date = fromSource(this.value);
        return Collections.singletonMap(this.key, toDisplay(date));
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> prop) {
        Object o = prop.get(this.key);
        if (o == null) return Collections.emptyMap();
        Date date = fromDisplay(o.toString());
        String sourceDate = toSource(date);
        if(sourceDate == null || sourceDate.equals(this.value)) return Collections.emptyMap();
        return null;
    }

    @Override
    protected boolean test(P predicate) {
        return predicate.test(toDisplay(fromSource(this.value)));
    }

    @Override
    public DateFormat getSourceDateFormat() {
        return sourceFormat;
    }

    @Override
    public DateFormat getDisplayDateFormat() {
        return displayFormat;
    }
}
