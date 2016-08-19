package org.unipop.schema.property;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface PropertySchema {
    String getKey();
    Map<String, Object> toProperties(Map<String, Object> source);
    Map<String, Object> toFields(Map<String, Object> properties);
    Set<String> toFields(Set<String> propertyKeys);
    Set<Object> getValues(PredicatesHolder predicatesHolder);
    default PredicatesHolder toPredicates(PredicatesHolder predicatesHolder){
        Stream<HasContainer> hasContainers = predicatesHolder.findKey(getKey());

        Set<PredicatesHolder> predicateHolders = hasContainers.map(this::toPredicate).collect(Collectors.toSet());
        return PredicatesHolderFactory.create(predicatesHolder.getClause(), predicateHolders);
    }
    default PredicatesHolder toPredicate(HasContainer hasContainer) { return null; }

    default Set<String> excludeDynamicFields() { return Collections.emptySet(); }
    default Set<String> excludeDynamicProperties() { return Collections.singleton(getKey()); }

    interface PropertySchemaBuilder {
        PropertySchema build(String key, Object conf, AbstractPropertyContainer container);
    }
}
