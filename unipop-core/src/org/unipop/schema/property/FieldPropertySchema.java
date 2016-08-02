package org.unipop.schema.property;

import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.json.JSONObject;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.util.ConversionUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class FieldPropertySchema implements PropertySchema {
    protected String key;
    protected String field = null;
    private boolean nullable;
    protected Set include;
    protected Set exclude;

    public FieldPropertySchema(String key, String field, boolean nullable) {
        this.key = key;
        this.field = field;
        this.nullable = nullable;
    }

    public FieldPropertySchema(String key, JSONObject config, boolean nullable) {
        this.key = key;
        this.nullable = nullable;
        this.field = config.getString("field");
        Set<Object> include = ConversionUtils.toSet(config, "include");
        this.include = include.isEmpty() ? null : include;
        Set<Object> exclude = ConversionUtils.toSet(config, "exclude");
        this.exclude = exclude.isEmpty() ? null : exclude;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        Object value = source.get(this.field);
        if (value == null && nullable) return Collections.emptyMap();
        if (value == null || !test(P.eq(value))) return null;
        return Collections.singletonMap(this.key, value);
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> properties) {
        Object value = properties.get(this.key);
        if (value == null && nullable) return Collections.emptyMap();
        if (value == null || !test(P.eq(value))) return null;
        return Collections.singletonMap(this.field, value);
    }

    @Override
    public Set<String> toFields(Set<String> keys) {
        if (keys.contains(this.key) || !nullable) return Collections.singleton(field);
        return Collections.emptySet();
    }

    @Override
    public PredicatesHolder toPredicate(HasContainer has) {
        P predicate;
        if (has != null && !test(has.getPredicate())) {
            return PredicatesHolderFactory.abort();
        } else if (has != null) {
            predicate = has.getPredicate();
        } else if (include != null) {
            predicate = P.within(include);
        } else if (exclude != null) {
            predicate = P.without(exclude);
        } else return PredicatesHolderFactory.empty();

        HasContainer hasContainer = new HasContainer(this.field, predicate);
        return PredicatesHolderFactory.predicate(hasContainer);
    }

    @Override
    public Set<String> excludeDynamicFields() {
        return Collections.singleton(this.field);
    }

    @Override
    public Set<String> excludeDynamicProperties() {
        return Collections.singleton(this.key);
    }

    private boolean test(P predicate) {


        if (this.include != null) {
            for (Object include : this.include) {
                if (predicate.test(include)) return true;
            }
            return false;
        }

        if (predicate.getBiPredicate() instanceof Contains) return true; //TODO: make this smarter.

        if (this.exclude != null) {
            for (Object exclude : this.exclude) {
                if (predicate.test(exclude))
                    return false;
            }
            return true;
        }
        return true;
    }

    @Override
    public String toString() {
        return "FieldPropertySchema{" +
                "exclude=" + exclude +
                ", key='" + key + '\'' +
                ", field='" + field + '\'' +
                ", nullable=" + nullable +
                ", include=" + include +
                '}';
    }

    public static class Builder implements PropertySchemaBuilder{
        @Override
        public PropertySchema build(String key, Object conf) {
            if (conf instanceof String){
                String field = conf.toString();
                if (!field.startsWith("@")) return null;
                return new FieldPropertySchema(key, field.substring(1), true);
            }
            if (!(conf instanceof JSONObject)) return null;
            JSONObject config = (JSONObject) conf;
            Object field = config.opt("field");
            if (field == null) return null;
            return new FieldPropertySchema(key, config, config.optBoolean("nullable", true));
        }
    }
}
