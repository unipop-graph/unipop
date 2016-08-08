package org.unipop.schema.property;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 8/2/16.
 */
public interface ParentSchemaProperty extends PropertySchema{
    Collection<PropertySchema> getChildren();

    default Set<String> excludeDynamicFields() {
        return getChildren().stream()
                .map(PropertySchema::excludeDynamicFields)
                .flatMap(Collection::stream).collect(Collectors.toSet());
    }

    default Set<String> toFields(Set<String> propertyKeys) {
        return getChildren().stream().flatMap(s -> s.toFields(propertyKeys).stream()).collect(Collectors.toSet());
    }

    default Map<String, Object> toFields(Map<String, Object> properties){
        return Collections.emptyMap();
    }
}
