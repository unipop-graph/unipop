package org.unipop.elastic.document.schema.property;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.json.JSONObject;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.schema.property.AbstractPropertyContainer;
import org.unipop.schema.property.ParentSchemaProperty;
import org.unipop.schema.property.PropertySchema;
import org.unipop.util.PropertySchemaFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 8/14/16.
 */
public class IndexPropertySchema implements ParentSchemaProperty {
    private PropertySchema schema;
    private String defaultIndex;
    private Set<String> createdIndices;
    private List<Validate> validations;

    public IndexPropertySchema(PropertySchema schema, String defaultIndex) {
        this.schema = schema;
        this.defaultIndex = defaultIndex;
        createdIndices = new HashSet<>();
        validations = new ArrayList<>();
    }

    public void addValidation(Validate validate){
        validations.add(validate);
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        if (predicatesHolder.getClause().equals(PredicatesHolder.Clause.Abort))
            return null;
        Set<Object> values = schema.getValues(predicatesHolder);
        Set<String> indices = values.size() > 0 ?
                values.stream().map(Object::toString).collect(Collectors.toSet()) :
                Collections.singleton(defaultIndex);
        return PredicatesHolderFactory
                .predicate(new HasContainer(getKey(), P.within(indices)));
    }

    @Override
    public Set<String> toFields(Set<String> propertyKeys) {
        return schema.toFields(propertyKeys);
    }

    @Override
    public Set<Object> getValues(PredicatesHolder predicatesHolder) {
        return null;
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> properties) {
        Map<String, Object> fields = schema.toProperties(properties);
        return Collections.singletonMap(getKey(), fields.values().iterator().next());
    }

    @Override
    public Collection<PropertySchema> getChildren() {
        return Collections.singleton(schema);
    }

    @Override
    public String getKey() {
        return "_index";
    }

    public List<String> getIndex(PredicatesHolder predicatesHolder) {
        Set<String> indices = predicatesToIndices(predicatesHolder);
        return new ArrayList<>(indices);
    }

    private Set<String> predicatesToIndices(PredicatesHolder predicatesHolder) {
        Set<Object> values = schema.getValues(predicatesHolder);
        return values.size() > 0 ? values.stream().map(Object::toString).collect(Collectors.toSet())
                : Collections.singleton(defaultIndex);
    }

    public String getIndex(Map<String, Object> fields) {
        String index = schema.toProperties(fields).values().iterator().next().toString();
        if (!createdIndices.contains(index))
            validations.forEach(v -> v.validate(index));
        return index;
    }

    public String getIndex() {
        return defaultIndex;
    }

    public boolean validateIndex(String index) {
        // TODO: validate index somehow
//        if (this.index.getIndex().contains("*"))
//            return index.matches(this.index.getIndex().replace("*", ".*"));
//        return this.index.getIndex().equals(index);
        return true;
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        return Collections.emptyMap();
    }

    public static class Builder implements PropertySchemaBuilder {
        @Override
        public PropertySchema build(String key, Object conf, AbstractPropertyContainer container) {
            if (key.equals("index")) {
                if (conf == null) return null;
                if (conf instanceof JSONObject) {
                    JSONObject config = (JSONObject) conf;
                    Object schema = config.opt("schema");
                    PropertySchema index = PropertySchemaFactory.createPropertySchema("_" + key, schema, container);
                    String defaultIndex = config.optString("default", "*");
                    return new IndexPropertySchema(index, defaultIndex);
                } else {
                    PropertySchema index = PropertySchemaFactory.createPropertySchema("_" + key, conf, container);
                    return new IndexPropertySchema(index, "*");
                }
            }
            return null;
        }
    }

    @FunctionalInterface
    public interface Validate{
        void validate(String index);
    }

    @Override
    public String toString() {
        return "IndexPropertySchema{" +
                "schema=" + schema +
                ", defaultIndex='" + defaultIndex + '\'' +
                '}';
    }
}
