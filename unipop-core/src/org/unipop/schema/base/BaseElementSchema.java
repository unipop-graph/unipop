package org.unipop.schema.base;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.schema.ElementSchema;
import org.unipop.schema.property.*;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public abstract class BaseElementSchema<E extends Element> implements ElementSchema<E> {

    protected final List<PropertySchema> propertySchemas;
    protected UniGraph graph;

    public BaseElementSchema(List<PropertySchema> propertySchemas, UniGraph graph) {
        this.propertySchemas = propertySchemas;
        this.graph = graph;
    }

    protected Map<String, Object> getProperties(Map<String, Object> source) {
        List<Map<String, Object>> fieldMaps = this.propertySchemas.stream().map(schema ->
                schema.toProperties(source)).collect(Collectors.toList());
        return merge(fieldMaps, this::mergeProperties, false);
    }

    protected Object mergeProperties(Object prop1, Object prop2) {
        if(!prop1.equals(prop2))
            System.out.println("merging unequal properties '" + prop1 + "' and '" + prop2 + "', schema: " + this);
        return prop1;
    }

    @Override
    public Map<String, Object> toFields(E element) {
        Map<String, Object> properties = UniElement.fullProperties(element);
        if(properties == null) return null;

        List<Map<String, Object>> fieldMaps = this.propertySchemas.stream().map(schema ->
                schema.toFields(properties)).collect(Collectors.toList());
        return merge(fieldMaps, this::mergeFields, false);
    }


    protected Object mergeFields(Object obj1, Object obj2) {
        if(!obj1.equals(obj2))
            System.out.println("merging unequal fields '" + obj1 + "' and '" + obj2 + "', schema: " + this);
        return obj1;
    }

    @Override
    public Set<String> toFields(Set<String> propertyKeys) {
        return propertySchemas.stream().flatMap(propertySchema ->
                propertySchema.toFields(propertyKeys).stream()).collect(Collectors.toSet());
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        Set<PredicatesHolder> predicates = propertySchemas.stream()
                .map(schema -> schema.toPredicates(predicatesHolder))
                .filter(holder -> holder != null)
                .collect(Collectors.toSet());

        return PredicatesHolderFactory.create(predicatesHolder.getClause(), predicates);
    }

    protected <K, V> Map<K, V> merge(List<Map<K, V>> maps, BiFunction<? super V, ? super V, ? extends V> mergeFunc, Boolean ignoreNull) {
        Map<K, V> newMap = new HashMap<>(maps.size());
        for(Map<K, V> current : maps) {
            if(current == null) {
                if (!ignoreNull) return null; //a null results indicates to cancel the merge.
                continue;
            }
            current.forEach((fieldKey, fieldValue) -> newMap.merge(fieldKey, fieldValue, mergeFunc));
        }
        return newMap;
    }
}
