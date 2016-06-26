package org.unipop.schema.base;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.schema.ElementSchema;
import org.unipop.schema.property.*;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;

public abstract class BaseElementSchema<E extends Element> implements ElementSchema<E> {

    protected final List<PropertySchema> propertySchemas;
    protected UniGraph graph;

    public List<PropertySchema> getPropertySchemas() {
        return propertySchemas;
    }

    public BaseElementSchema(List<PropertySchema> propertySchemas, UniGraph graph) {
        this.propertySchemas = propertySchemas;
        this.graph = graph;
    }

    protected Map<String, Object> getProperties(Map<String, Object> source) {
        Map<String, Object> result = new HashMap<>();
        for(PropertySchema schema : this.propertySchemas) {
            Map<String, Object> schemaProperties = schema.toProperties(source);
            if(schemaProperties == null) return null;
            schemaProperties.forEach((propKey, propValue) -> result.merge(propKey, propValue, this::mergeProperties));
        }

        return result;
    }

    protected Object mergeProperties(Object prop1, Object prop2) {
        return prop1;
    }

    @Override
    public Map<String, Object> toFields(E element) {
        Map<String, Object> properties = UniElement.fullProperties(element);
        assert properties != null;

        Map<String, Object> fields = new HashMap<>();
        for(PropertySchema schema : this.propertySchemas) {
            Map<String, Object> schemaFields = schema.toFields(properties);
            if(schemaFields == null) return null;
            schemaFields.forEach((fieldKey, fieldValue) -> fields.merge(fieldKey, fieldValue, this::mergeFields));
        }

        return fields;
    }

    protected Object mergeFields(Object obj1, Object obj2) {
        return obj1;
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        Set<PredicatesHolder> predicates = predicatesHolder.getPredicates().stream()
                .map(this::convertPredicate).collect(Collectors.toSet());

        Set<PredicatesHolder> children = predicatesHolder.getChildren().stream()
                .map(this::toPredicates).collect(Collectors.toSet());

        predicates.addAll(children);

        return PredicatesHolderFactory.create(predicatesHolder.getClause(), predicates);
    }

    private PredicatesHolder convertPredicate(HasContainer hasContainer) {
        return propertySchemas.stream()
                .map(schema -> schema.toPredicates(hasContainer))
                .filter(predicatesHolder -> predicatesHolder != null)
                .findFirst().orElse(PredicatesHolderFactory.abort());
    }
}
