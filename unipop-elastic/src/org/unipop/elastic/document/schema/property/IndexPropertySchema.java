package org.unipop.elastic.document.schema.property;

import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONObject;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.schema.property.ParentSchemaProperty;
import org.unipop.schema.property.PropertySchema;
import org.unipop.util.PropertySchemaFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 8/14/16.
 */
public class IndexPropertySchema implements ParentSchemaProperty {
    private PropertySchema schema;
    private String defaultIndex;

    public IndexPropertySchema(PropertySchema schema, String defaultIndex) {
        this.schema = schema;
        this.defaultIndex = defaultIndex;
    }

    @Override
    public Collection<PropertySchema> getChildren() {
        return Collections.singleton(schema);
    }

    @Override
    public String getKey() {
        return "index";
    }

    public String getIndex(PredicatesHolder predicatesHolder){
        Map<String, Object> fields = predicatesToFields(predicatesHolder);
        if (fields.size() == 0) return defaultIndex;
        return schema.toProperties(fields).values().iterator().next().toString();
    }

    private Map<String, Object> predicatesToFields(PredicatesHolder predicatesHolder) {
         return predicatesHolder.getPredicates().stream()
                 .map(has -> Pair.of(has.getKey(), has.getValue()))
                 .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    }

    public String getIndex(Map<String, Object> fields){
        return schema.toProperties(fields).values().iterator().next().toString();
    }

    public String getIndex(){
        return defaultIndex;
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        return null;
    }

    public static class Builder implements PropertySchemaBuilder{
        @Override
        public PropertySchema build(String key, Object conf) {
            if (key.equals("index") || key.equals("_index")){
                if (conf instanceof JSONObject) {
                    JSONObject config = (JSONObject) conf;
                    Object schema = config.opt("schema");
                    PropertySchema index = PropertySchemaFactory.createPropertySchema(key + "_schema", schema);
                    String defaultIndex = config.optString("default", "*");
                    return new IndexPropertySchema(index, defaultIndex);
                }
                else {
                    PropertySchema index = PropertySchemaFactory.createPropertySchema(key + "_schema", conf);
                    return new IndexPropertySchema(index, "*");
                }
            }
            return null;
        }
    }
}
