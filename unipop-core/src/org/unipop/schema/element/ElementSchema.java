package org.unipop.schema.element;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.schema.property.PropertySchema;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * A schema representing an element
 * @param <E> An element
 */
public interface ElementSchema<E extends Element> {
    /**
     * Returns a collection of elements from the data source fields
     * @param fields The data source fields
     * @return Collection of elements
     */
    Collection<E> fromFields(Map<String, Object> fields);

    /**
     * Converts an element to data source field
     * @param element An element
     * @return A map of data source fields
     */
    Map<String, Object> toFields(E element);

    /**
     * Converts property keys to field names
     * @param propertyKeys A set of property keys
     * @return A set of field names
     */
    Set<String> toFields(Set<String> propertyKeys);

    /**
     * Converts predicate to match data source
     * @param predicatesHolder A predicate holder
     * @return Converted predicate holder
     */
    PredicatesHolder toPredicates(PredicatesHolder predicatesHolder);

    /**
     * Converts a property key to field name
     * @param key Property key
     * @return Field name
     */
    String getFieldByPropertyKey(String key);

    /**
     * TODO: add java doc
     * @param key
     * @return
     */
    PropertySchema getPropertySchema(String key);

    /**
     * Returns child schemas
     * @return A set of schemas
     */
    default Set<ElementSchema> getChildSchemas() { return Collections.emptySet(); }
}
