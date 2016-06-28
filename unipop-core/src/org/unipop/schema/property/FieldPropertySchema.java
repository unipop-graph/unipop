package org.unipop.schema.property;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.unipop.util.ConversionUtils;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.util.*;

public class FieldPropertySchema implements PropertySchema {
    private String key;
    private String field = null;
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
        JSONArray include = config.optJSONArray("include");
        if(include != null) this.include = ConversionUtils.toSet(include);
        JSONArray exclude = config.optJSONArray("exclude");
        if(exclude != null) this.exclude = ConversionUtils.toSet(exclude);
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        Object value = source.get(this.field);
        if(value == null && nullable) return Collections.emptyMap();
        if(value == null || !test(P.eq(value))) return null;
        return Collections.singletonMap(this.key, value);
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> properties) {
        Object value = properties.get(this.key);
        if(value == null && nullable) return Collections.emptyMap();
        if(value == null || !test(P.eq(value))) return null;
        return Collections.singletonMap(this.field, value);
    }

    @Override
    public Set<String> toFields(Set<String> keys) {
        if(keys.contains(this.key) || !nullable) return Collections.singleton(field);
        return Collections.emptySet();
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        HasContainer has = predicatesHolder.findKey(this.key);

        P predicate;
        if (has != null && !test(has.getPredicate())) {
            return PredicatesHolderFactory.abort();
        }
        else if(has != null) {
            predicate = has.getPredicate();
        }
        else if(include != null) {
            predicate = P.within(include);
        }
        else if(exclude != null) {
            predicate = P.without(exclude);
        } else return PredicatesHolderFactory.empty();

        HasContainer hasContainer = new HasContainer(this.field, predicate);
        return PredicatesHolderFactory.predicate(hasContainer);
    }

    @Override
    public Set<String> getFields() {
        return Collections.singleton(this.field);
    }

    @Override
    public Set<String> getProperties() {
        return Collections.singleton(this.key);
    }

    private boolean test(P predicate) {
        if(this.include != null){
            for(Object include : this.include) {
                if(predicate.test(include)) return true;
            }
            return false;
        }
        if(this.exclude != null) {
            for (Object exclude : this.exclude) {
                if (predicate.test(exclude)) return false; //TODO: handle mixed results
            }
            return true;
        }
        return true;
    }
}
