package org.unipop.schema.property;

import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.json.JSONObject;
import org.unipop.process.predicate.Date;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.schema.property.type.PropertyType;
import org.unipop.schema.property.type.TextType;
import org.unipop.util.ConversionUtils;
import org.unipop.util.PropertyTypeFactory;

import java.util.*;
import java.util.stream.Stream;

public class FieldPropertySchema implements PropertySchema {
    protected String key;
    protected String field = null;
    protected boolean nullable;
    protected Set include;
    protected Set exclude;
    protected PropertyType type;
    protected JSONObject alias;
    protected Map<String, String> reverseAlias;

    public FieldPropertySchema(String key, String field, boolean nullable) {
        this.key = key;
        this.field = field;
        this.nullable = nullable;
        try {
            this.type = PropertyTypeFactory.getType("STRING");
        } catch (IllegalAccessException | InstantiationException e) {
            e.printStackTrace();
        }
        this.alias = null;
        this.reverseAlias = null;
    }

    public FieldPropertySchema(String key, JSONObject config, boolean nullable) {
        this.key = key;
        this.nullable = nullable;
        this.field = config.getString("field");
        Set<Object> include = ConversionUtils.toSet(config, "include");
        this.include = include.isEmpty() ? null : include;
        Set<Object> exclude = ConversionUtils.toSet(config, "exclude");
        this.exclude = exclude.isEmpty() ? null : exclude;
        String typeName = config.optString("type", "STRING");
        try {
            this.type = PropertyTypeFactory.getType(typeName);
        } catch (IllegalAccessException | InstantiationException e) {
            e.printStackTrace();
        }
        if (include != null || exclude != null)
            this.nullable = false;
        Object alias = config.opt("alias");
        this.alias = alias == null ? null : ((JSONObject) alias);
        if (this.alias == null)
            reverseAlias = null;
        else{
            reverseAlias = new HashMap<>();
            this.alias.keys().forEachRemaining(aliasKey -> reverseAlias.put(this.alias.getString(aliasKey), aliasKey));
        }
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        Object value = source.get(this.field);
        if (value == null && nullable) return Collections.emptyMap();
        if (alias != null) value = alias.has(value.toString()) ? alias.get(value.toString()) : value;
        if (value == null || !test(P.eq(value))) return null;
        return Collections.singletonMap(this.key, value);
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> properties) {
        Object value = properties.get(this.key);
        if (value == null && nullable) return Collections.emptyMap();
        // TODO: add alias use
        if (value == null || !test(P.eq(value))) return null;
        return Collections.singletonMap(this.field, value);
    }

    @Override
    public Set<String> toFields(Set<String> keys) {
        if (keys.contains(this.key) || !nullable) return Collections.singleton(field);
        return Collections.emptySet();
    }

    @Override
    public Set<Object> getValues(PredicatesHolder predicatesHolder) {
        Stream<HasContainer> predicates = predicatesHolder.findKey(this.key);
        Optional<Object> value = predicates.map(HasContainer::getValue).findFirst();
        return value.isPresent() ? Collections.singleton(value.get()) : null;
    }

    @Override
    public PredicatesHolder toPredicate(HasContainer has) {
        P predicate;
        if (has != null && !test(has.getPredicate())) {
            return PredicatesHolderFactory.abort();
        } else if (has != null) {
            predicate = has.getPredicate().clone();
            if (reverseAlias != null) {
                Object predicateValue = predicate.getValue();
                if (reverseAlias.containsKey(predicateValue.toString())){
                    predicate.setValue(reverseAlias.get(predicateValue.toString()));
                }
            }
        } else if (include != null) {
            predicate = P.within(include);
        } else if (exclude != null) {
            predicate = P.without(exclude);
        } else return PredicatesHolderFactory.empty();

        P translatedPredicate = type.translate(predicate);

        HasContainer hasContainer = new HasContainer(this.field, translatedPredicate);
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

    protected boolean test(P predicate) {


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
        public PropertySchema build(String key, Object conf, AbstractPropertyContainer container) {
            boolean nullable = !(key.equals("~id") || key.equals("~label"));
            if (conf instanceof String){
                String field = conf.toString();
                if (!field.startsWith("@")) return null;
                return new FieldPropertySchema(key, field.substring(1), nullable);
            }
            if (!(conf instanceof JSONObject)) return null;
            JSONObject config = (JSONObject) conf;
            Object field = config.opt("field");
            if (field == null) return null;
            return new FieldPropertySchema(key, config, config.optBoolean("nullable", nullable));
        }
    }
}
