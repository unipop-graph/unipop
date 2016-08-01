package org.unipop.schema.property;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.json.JSONObject;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

/**
 * Created by sbarzilay on 8/1/16.
 */
public class DatePropertySchema extends FieldPropertySchema {
    protected SimpleDateFormat sourceFormat;
    protected SimpleDateFormat displayFormat;

    public DatePropertySchema(String key, String field, String format, boolean nullable) {
        super(key, field, nullable);
        this.sourceFormat = new SimpleDateFormat(format);
        this.displayFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
    }

    public DatePropertySchema(String key, JSONObject config, boolean nullable) {
        super(key, config, nullable);
        this.sourceFormat = new SimpleDateFormat(config.optString("sourceFormat"));
        this.displayFormat = new SimpleDateFormat(config.optString("displayFormat", "yyyy-MM-dd HH:mm:ss:SSS"));
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        Object dateField = source.get(this.field);
        if (dateField == null) return Collections.emptyMap();
        try {
            Date parsedDate = sourceFormat.parse(dateField.toString());
            String displayDate = displayFormat.format(parsedDate);
            return Collections.singletonMap(this.key, displayDate);
        } catch (ParseException e) {
            throw new RuntimeException(String.format("couldn't parse date:{0} using:{1}", dateField, sourceFormat.toPattern()));
        }
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> properties) {
        Object dateProperty = properties.get(key);
        if (dateProperty == null) return Collections.emptyMap();
        try {
            Date parsedDate = displayFormat.parse(dateProperty.toString());
            String sourceDate = sourceFormat.format(parsedDate);
            return Collections.singletonMap(this.field, sourceDate);
        } catch (ParseException e) {
            throw new RuntimeException(String.format("couldn't parse date:%s using:%s", dateProperty, displayFormat.toPattern()));
        }
    }

    @Override
    protected PredicatesHolder toPredicate(HasContainer has) {
        Object dateValue = has.getValue();
        try {
            Date parsedDate = displayFormat.parse(dateValue.toString());
            String formattedDate = sourceFormat.format(parsedDate);
            P predicate = has.getPredicate().clone();
            predicate.setValue(formattedDate);
            return PredicatesHolderFactory.predicate(new HasContainer(this.field, predicate));
        } catch (ParseException e) {
            throw new RuntimeException(String.format("couldn't parse date:{0} using:{1}", dateValue, displayFormat.toPattern()));
        }
    }
}
