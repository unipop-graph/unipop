package org.unipop.schema.property;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.json.JSONObject;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

/**
 * Created by sbarzilay on 8/1/16.
 */
public class DateFieldPropertySchema extends FieldPropertySchema implements DatePropertySchema {
    protected final SimpleDateFormat sourceFormat;
    protected final SimpleDateFormat displayFormat;

    public DateFieldPropertySchema(String key, String field, String format, boolean nullable) {
        super(key, field, nullable);
        this.sourceFormat = new SimpleDateFormat(format);
        this.displayFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
    }

    public DateFieldPropertySchema(String key, JSONObject config, boolean nullable) {
        super(key, config, nullable);
        this.sourceFormat = new SimpleDateFormat(config.optString("sourceFormat"));
        this.displayFormat = new SimpleDateFormat(config.optString("displayFormat", "yyyy-MM-dd HH:mm:ss:SSS"));
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        Object dateField = source.get(this.field);
        if (dateField == null) return Collections.emptyMap();
        Date parsedDate = fromSource(dateField.toString());
        String displayDate = toDisplay(parsedDate);
        return Collections.singletonMap(this.key, displayDate);
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> properties) {
        Object dateProperty = properties.get(key);
        if (dateProperty == null) return Collections.emptyMap();
        Date parsedDate = fromDisplay(dateProperty.toString());
        String sourceDate = toSource(parsedDate);
        return Collections.singletonMap(this.field, sourceDate);
    }

    @Override
    protected PredicatesHolder toPredicate(HasContainer has) {
        Object dateValue = has.getValue();
        Date parsedDate = fromDisplay(dateValue.toString());
        String formattedDate = toSource(parsedDate);
        P predicate = has.getPredicate().clone();
        predicate.setValue(formattedDate);
        return PredicatesHolderFactory.predicate(new HasContainer(this.field, predicate));
    }

    @Override
    public DateFormat getSourceDateFormat() {
        return sourceFormat;
    }

    @Override
    public DateFormat getDisplayDateFormat() {
        return displayFormat;
    }

    public static class Builder implements PropertySchemaBuilder {
        @Override
        public PropertySchema build(String key, Object conf) {
            if (!(conf instanceof JSONObject)) return null;
            JSONObject config = (JSONObject) conf;
            Object field = config.opt("field");
            Object format = config.opt("sourceFormat");
            if (field == null || format == null) return null;
            return new DateFieldPropertySchema(key, config, config.optBoolean("nullable", true));
        }
    }
}
