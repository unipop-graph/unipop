package org.unipop.schema;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.schema.property.PropertySchema;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;
import org.unipop.util.ConversionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public interface ElementSchema<E extends Element> {
    UniGraph getGraph();
    E fromFields(Map<String, Object> fields);
    List<PropertySchema> getPropertySchemas();

    default Set<ElementSchema> getWithChildSchemas() {
        return Collections.singleton(this);
    }

    default Map<String, Object> getProperties(Map<String, Object> source) {
        List<Map<String, Object>> fieldMaps = this.getPropertySchemas().stream().map(schema ->
                schema.toProperties(source)).collect(Collectors.toList());
        return ConversionUtils.merge(fieldMaps, this::mergeProperties, false);
    }

    default Object mergeProperties(Object prop1, Object prop2) {
        if(!prop1.equals(prop2))
            System.out.println("merging unequal properties '" + prop1 + "' and '" + prop2 + "', schema: " + this);
        return prop1;
    }

    default Map<String, Object> toFields(E element) {
        return getFields(element);
    }

    default Map<String, Object> getFields(E element) {
        Map<String, Object> properties = UniElement.fullProperties(element);
        if(properties == null) return null;

        List<Map<String, Object>> fieldMaps = this.getPropertySchemas().stream().map(schema ->
                schema.toFields(properties)).collect(Collectors.toList());
        return ConversionUtils.merge(fieldMaps, this::mergeFields, false);
    }


    default Object mergeFields(Object obj1, Object obj2) {
        if(!obj1.equals(obj2))
            System.out.println("merging unequal fields '" + obj1 + "' and '" + obj2 + "', schema: " + this);
        return obj1;
    }

    default Set<String> toFields(Set<String> propertyKeys) {
        return getPropertySchemas().stream().flatMap(propertySchema ->
                propertySchema.toFields(propertyKeys).stream()).collect(Collectors.toSet());
    }

    default PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        Set<PredicatesHolder> predicates = getPropertySchemas().stream()
                .map(schema -> schema.toPredicates(predicatesHolder))
                .filter(holder -> holder != null)
                .collect(Collectors.toSet());

        return PredicatesHolderFactory.create(predicatesHolder.getClause(), predicates);
    }
}
