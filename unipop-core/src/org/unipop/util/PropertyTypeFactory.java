package org.unipop.util;

import org.unipop.schema.property.type.PropertyType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by sbarzilay on 8/19/16.
 */
public class PropertyTypeFactory {
    private static Set<PropertyType> propertyTypes;

    public static void init(List<String> types){
        propertyTypes = new HashSet<>();
        for (String type : types) {
            try {
                PropertyType propertyType = Class.forName(type).asSubclass(PropertyType.class).newInstance();
                propertyTypes.add(propertyType);
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                throw new IllegalArgumentException("class: '" + type + "' not found");
            }
        }
    }

    public static PropertyType getType(String typeName) throws IllegalAccessException, InstantiationException {
        for (PropertyType propertyType : propertyTypes) {
            if (propertyType.getType().equals(typeName.toUpperCase())) return propertyType;
        }
        throw new IllegalArgumentException("Property type: '" + typeName + "' does not exists");
    }
}
