package org.unipop.schema.property;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A schema which represents a property
 */
public interface PropertySchema {

    /**
     * Returns the property key
     * @return Key
     */
    String getKey();

    /**
     * Converts a map of fields to a map of properties
     * @param source A map of fields
     * @return A map of properties
     */
    Map<String, Object> toProperties(Map<String, Object> source);

    /**
     * Converts a map of properties to a map of fields
     * @param properties A map of properties
     * @return A map of fields
     */
    Map<String, Object> toFields(Map<String, Object> properties);

    /**
     * Converts property keys to data source field names
     * @param propertyKeys Property keys
     * @return A set of field names
     */
    Set<String> toFields(Set<String> propertyKeys);

    /**
     * Returns possible values from predicate holder
     * @param predicatesHolder Predicates holder
     * @return A set of possible values
     */
    Set<Object> getValues(PredicatesHolder predicatesHolder);

    /**
     * Converts a predicate to match the source field
     * @param predicatesHolder Predicates holder
     * @return A converted predicates holder
     */
    default PredicatesHolder toPredicates(PredicatesHolder predicatesHolder){
        Stream<HasContainer> hasContainers = predicatesHolder.findKey(getKey());

        Set<PredicatesHolder> predicateHolders = hasContainers.map(this::toPredicate).collect(Collectors.toSet());
        return PredicatesHolderFactory.create(predicatesHolder.getClause(), predicateHolders);
    }

    /**
     * Convert a has container into a predicate
     * @param hasContainer Has container
     * @return Predicates holder
     */
    default PredicatesHolder toPredicate(HasContainer hasContainer) { return null; }

    /**
     * Excludes fields from dynamic fields
     * @return A set of excluded fields
     */
    default Set<String> excludeDynamicFields() { return Collections.emptySet(); }

    /**
     * Excludes fields from dynamic properties
     * @return A set of excluded properties
     */
    default Set<String> excludeDynamicProperties() { return Collections.singleton(getKey()); }

    /**
     * Property schema builder
     */
    interface PropertySchemaBuilder {
        /**
         * Builds the property schema
         * @param key Property key
         * @param conf Mapping
         * @param container Property Container
         * @return Property Schema
         */
        PropertySchema build(String key, Object conf, AbstractPropertyContainer container);
    }
}
