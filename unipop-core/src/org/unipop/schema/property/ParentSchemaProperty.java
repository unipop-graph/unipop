package org.unipop.schema.property;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A property schema which contains child schemas
 * Created by sbarzilay on 8/2/16.
 */
public interface ParentSchemaProperty extends PropertySchema{

    /**
     * Returns child schemas
     * @return A collection of property schemas
     */
    Collection<PropertySchema> getChildren();

    /**
     * Excludes fields from dynamic fields
     * @return A set of excluded fields
     */
    default Set<String> excludeDynamicFields() {
        return getChildren().stream()
                .map(PropertySchema::excludeDynamicFields)
                .flatMap(Collection::stream).collect(Collectors.toSet());
    }

    /**
     * Converts property keys to data source field names
     * @param propertyKeys Property keys
     * @return A set of field names
     */
    default Set<String> toFields(Set<String> propertyKeys) {
        return getChildren().stream().flatMap(s -> s.toFields(propertyKeys).stream()).collect(Collectors.toSet());
    }

    /**
     * Converts properties into data source fields
     * @param properties A map of properties
     * @return A map of fields
     */
    default Map<String, Object> toFields(Map<String, Object> properties){
        return Collections.emptyMap();
    }
}
