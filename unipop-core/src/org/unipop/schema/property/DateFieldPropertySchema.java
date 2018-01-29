package org.unipop.schema.property;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.json.JSONArray;
import org.json.JSONObject;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.util.ConversionUtils;
import org.unipop.util.MultiDateFormat;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by sbarzilay on 8/1/16.
 */
public class DateFieldPropertySchema extends FieldPropertySchema implements DatePropertySchema {
    protected final String sourceFormat;
    protected final List<String> displayFormat;
    protected long interval;

    public DateFieldPropertySchema(String key, String field, String format, boolean nullable) {
        super(key, field, nullable);
        this.sourceFormat = format;
        this.displayFormat = Collections.singletonList("yyyy-MM-dd HH:mm:ss:SSS");
        this.interval = 1000 * 60 * 60 *24;
    }

    public DateFieldPropertySchema(String key, JSONObject config, boolean nullable) {
        super(key, config, nullable);
        this.sourceFormat = config.optString("sourceFormat");
        JSONArray displayFormats = config.optJSONArray("displayFormats");
        if (displayFormats == null){
            this.displayFormat = Collections.singletonList("yyyy-MM-dd HH:mm:ss:SSS");
        } else{
            List<String> formats = ConversionUtils.asStream(displayFormats.iterator())
                    .map(Object::toString).collect(Collectors.toList());
            this.displayFormat = formats;
        }
        String interval = config.optString("interval", "1d");
        if (interval.matches("\\d+")){
            this.interval = Long.parseLong(interval);
        } else {
            if (interval.endsWith("d")) {
                this.interval = Long.parseLong(interval.substring(0, interval.length() - 1))
                        * 1000 * 60 * 60 * 24;
            } else if (interval.endsWith("h")) {
                this.interval = Long.parseLong(interval.substring(0, interval.length() - 1))
                        * 1000 * 60 * 60;
            } else if (interval.endsWith("m")) {
                this.interval = Long.parseLong(interval.substring(0, interval.length() - 1))
                        * 1000 * 60;
            } else if (interval.endsWith("s")) {
                this.interval = Long.parseLong(interval.substring(0, interval.length() - 1))
                        * 1000;
            }
        }
    }

    @Override
    public Set<Object> getValues(PredicatesHolder predicatesHolder) {
        Stream<PredicatesHolder> predicates = predicatesHolder.findKey(this.key).map(this::explodeConnective);
        Map<String, Date> datePredicates = new HashMap<>();
        predicates.flatMap(p -> p.getPredicates().stream())
                .forEach(has -> {
                    String biPredicate = has.getBiPredicate().toString();
                    Object value = has.getValue();
                    switch (biPredicate) {
                        case "eq":
                            datePredicates.put("eq", fromDisplay(value.toString()));
                            break;
                        case "gt":
                            datePredicates.put("gt", fromDisplay(value.toString()));
                        case "gte":
                            datePredicates.put("gte", fromDisplay(value.toString()));
                            break;
                        case "lt":
                            datePredicates.put("lt", fromDisplay(value.toString()));
                            break;
                        case "lte":
                            datePredicates.put("lte", fromDisplay(value.toString()));
                            break;
                        default:
                            throw new IllegalArgumentException("cant get value");
                    }
                });
        if (datePredicates.size() == 0) return Collections.emptySet();
        if (datePredicates.containsKey("eq")) return Collections.singleton(toSource(datePredicates.get("eq")));
        else if ((datePredicates.containsKey("gt") || datePredicates.containsKey("gte"))
                && (datePredicates.containsKey("lt") || datePredicates.containsKey("lte"))) {
            Date from = datePredicates.containsKey("gt") ? datePredicates.get("gt") : datePredicates.get("gte");
            Date to = datePredicates.containsKey("lt") ? datePredicates.get("lt") : datePredicates.get("lte");
            List<Date> dates = new ArrayList<>();
            long interval = this.interval;
            long endTime = to.getTime();
            long curTime = from.getTime();
            while (curTime <= endTime) {
                dates.add(new Date(curTime));
                curTime += interval;
            }
            return dates.stream().map(this::toSource).collect(Collectors.toSet());
        }
        else throw new IllegalArgumentException("cant get only gt or lt value");
    }

    public PredicatesHolder explodeConnective(HasContainer has) {
        if (has.getBiPredicate() instanceof ConnectiveP) {
            List<P> predicates = ((ConnectiveP) has.getBiPredicate()).getPredicates();
            PredicatesHolder.Clause clause = has.getPredicate() instanceof AndP ?
                    PredicatesHolder.Clause.And : PredicatesHolder.Clause.Or;
            Set<HasContainer> hasContainers = predicates.stream()
                    .map(p -> new HasContainer(has.getKey(), p)).collect(Collectors.toSet());
            return PredicatesHolderFactory.createFromPredicates(clause, hasContainers);
        }
        return PredicatesHolderFactory.predicate(has);
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        Object dateField = source.get(this.field);
        if (dateField == null || dateField.equals("")) return Collections.emptyMap();
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
    public PredicatesHolder toPredicate(HasContainer has) {
        if (has.getPredicate() instanceof ConnectiveP) {
            List<P> predicates = ((ConnectiveP) has.getPredicate()).getPredicates();
            predicates.forEach(p -> {
                Object dateValue = p.getValue();
                Date parsedDate = fromDisplay(dateValue.toString());
                String formattedDate = toSource(parsedDate);
                p.setValue(formattedDate);
            });
            return PredicatesHolderFactory
                    .predicate(new HasContainer(this.field, has.getPredicate()));
        }
        Object dateValue = has.getValue();
        Date parsedDate = fromDisplay(dateValue.toString());
        String formattedDate = toSource(parsedDate);
        P predicated = has.getPredicate().clone();
        predicated.setValue(formattedDate);
        return PredicatesHolderFactory.predicate(new HasContainer(this.field, predicated));
    }

    @Override
    public DateFormat getSourceDateFormat() {
        return new SimpleDateFormat(sourceFormat);
    }

    @Override
    public DateFormat getDisplayDateFormat() {
        if (this.displayFormat.size() > 1) {
            return new MultiDateFormat(displayFormat.get(0),
                    displayFormat.subList(1, displayFormat.size() -1));
        }
        return new MultiDateFormat(displayFormat.get(0), Collections.emptyList());
    }

    public static class Builder implements PropertySchemaBuilder {
        @Override
        public PropertySchema build(String key, Object conf, AbstractPropertyContainer container) {
            if (!(conf instanceof JSONObject)) return null;
            JSONObject config = (JSONObject) conf;
            Object field = config.opt("field");
            Object format = config.opt("sourceFormat");
            if (field == null || format == null) return null;
            return new DateFieldPropertySchema(key, config, config.optBoolean("nullable", true));
        }
    }
}